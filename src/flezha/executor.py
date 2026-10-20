"""The concurrency host: what runs ``poll -> dispatch -> complete``, and on what.

Two members in v1, and the seam between them may never observe or affect
per-cell ordering. Exclusivity is the core's handout guarantee — one
outstanding activation per Cell — and an activation is completed only after
its dispatch has returned, so ordering holds for any executor that does not
complete early. That invariant is this seam's entire contract: an executor can
affect throughput and latency; it cannot affect correctness.

How many threads is the core's pool governor's to say: this host opens the
pool over its processor count and answers its supervisor's decisions. Three
things are shared rather than written once per executor, because they are
properties of the host and not of either member: the threads and their bounded
drain (:class:`_Pool`), the fetch-and-run worker (:func:`_fetch_and_run`), and
the supervisor (:meth:`_Host._supervise`) — whose answer to a grown seat is one
more fetch-and-run **thread** under either executor, because a thread is what a
parked completion needs and a stalled loop cannot supply.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time
from collections.abc import Callable
from typing import Any, Protocol

from .dispatch import DispatchDeps, close_thread_loop, dispatch, dispatch_now
from .errors import FlezhaError
from .logger import log_notices, logger

__all__ = [
    "Executor",
    "ThreadExecutor",
    "AsyncioExecutor",
    "build",
    "EXECUTORS",
]

_log = logger("executor")

#: How far the asyncio executor's fetchers may run ahead of its loop. The core's
#: in-flight cap bounds the total either way; this only stops one fetcher
#: monopolising the hand-off.
QUEUE_DEPTH = 64

#: What ``pool_supervise`` answers: spawn a base worker, stop, spawn a grown one;
#: anything else is nothing to do this wait.
POOL_SPAWN_BASE = 1
POOL_STOP = 2
POOL_SPAWN_GROWN = 3

#: What ``poll`` answers when every poller has been released.
SHUTTING_DOWN = "shutting_down"


class Executor(Protocol):
    """Drive the poll loop; ``start`` MAY drive it on N threads."""

    @property
    def name(self) -> str: ...

    @property
    def size(self) -> int: ...

    def describe(self) -> str: ...

    def start(self) -> Executor: ...

    def stop_fetching(self) -> None: ...

    def stop(self) -> int: ...


def _remaining(deadline: float) -> float:
    """How long is left of a drain, never negative."""
    return max(deadline - time.monotonic(), 0.0)


def _report_undrained(count: int, drain_s: float) -> None:
    """Say what the drain did not finish.

    Silence here is the failure mode the bound exists to avoid reintroducing: a
    runner that leaves without reporting what it abandoned looks identical to
    one that drained.
    """
    if not count:
        return
    _log.warning(
        "%d activation(s) were still running after the %gs drain; the runner is exiting "
        "anyway. The core re-hands unanswered work on the next bind — find the handler that "
        "does not return.",
        count,
        drain_s,
    )


def fetch(
    core: Any,
    poll_ms: int,
    stopping: Callable[[], bool],
    on_activation: Callable[[Any], Any],
    on_core_shutdown: Callable[[], None] = lambda: None,
) -> None:
    """One fetch loop, and the one place ``poll``'s three answers are read.

    Both executors fetch, so what a timeout, an activation and a shutdown each
    mean lives here rather than in one correct copy per executor. The callback
    is handed each activation, and ``None`` on a timeout so a caller that
    retires on an idle streak can count them; answering ``"stop"`` ends the
    loop, as does ``stopping()``.

    A core that says it is shutting down ends the loop AND reports it: a
    controller-initiated close is not the host's own stop, and a runner that
    only watched its own would keep waiting for a termination nothing was going
    to signal.
    """
    while not stopping():
        activation = core.poll(poll_ms)
        if activation == SHUTTING_DOWN:
            on_core_shutdown()
            return
        if on_activation(activation) == "stop":
            return


def _fetch_and_run(
    core: Any,
    deps: DispatchDeps,
    stopping: Callable[[], bool],
    *,
    poll_ms: int,
    retire_ms: int,
    on_core_shutdown: Callable[[], None] = lambda: None,
) -> None:
    """One worker: ``poll -> dispatch -> complete``, on this thread.

    The body of both the thread executor's base pool and the grown worker the
    supervisor starts under either executor, because those are the same
    worker — what differs is only whether it retires after ``retire_ms`` of
    timed-out polls (a base worker passes 0 and never does). Its seat goes back
    to the governor however it ends.
    """
    idle = 0

    def served(activation: Any) -> Any:
        nonlocal idle
        if activation is None:
            idle += poll_ms
            return "stop" if retire_ms and idle >= retire_ms else None
        idle = 0
        dispatch_now(activation, core, deps)
        return None

    try:
        fetch(core, poll_ms, stopping, served, on_core_shutdown)
    except Exception as error:  # noqa: BLE001 - a silent death is the defect
        # A worker that dies silently is a pool that shrinks with no report;
        # the governor replaces it, but only this line says why it left.
        _log.error("a worker thread ended on %s", error, exc_info=error)
    finally:
        close_thread_loop()
        core.pool_left()


class _Pool:
    """The threads an executor owns, and the bounded drain that ends them."""

    def __init__(self) -> None:
        self._threads: list[threading.Thread] = []
        self._lock = threading.Lock()

    def spawn(self, name: str, body: Callable[[], None]) -> threading.Thread:
        thread = threading.Thread(target=body, name=name, daemon=True)
        with self._lock:
            # Retired workers are dropped here rather than at the drain: a
            # long-lived runner that grows and retires through many starvation
            # episodes would otherwise accumulate a dead Thread per episode for
            # the life of the process.
            self._threads = [t for t in self._threads if t.is_alive()]
            self._threads.append(thread)
        thread.start()
        return thread

    def alive(self) -> int:
        with self._lock:
            return sum(1 for thread in self._threads if thread.is_alive())

    def join(self, deadline: float) -> int:
        """Join every thread within the deadline; answer how many outlived it."""
        with self._lock:
            threads = list(self._threads)
        for thread in threads:
            thread.join(_remaining(deadline))
        undrained = sum(1 for thread in threads if thread.is_alive())
        with self._lock:
            self._threads = [thread for thread in self._threads if thread.is_alive()]
        return undrained


class _Host:
    """What both executors are: a pool the core's governor sizes, its
    supervisor, and a bounded drain.

    The supervisor is not an optimisation and not optional. A completion can
    block: an emit onto a full ``DropNothing`` consumer mailbox parks the
    completing thread, and ``routing::install_inline_serve`` — the hook that
    rescues the in-process Rust host by draining the consumer inline — takes a
    Rust function pointer and is not reachable over the C ABI. A host whose
    only thread is parked in a completion is therefore not entering ``poll``,
    which is exactly what the core reports as starvation, and the worker the
    governor then asks for is what drains the consumer and releases the
    producer.
    """

    def __init__(
        self,
        core: Any,
        deps: DispatchDeps,
        *,
        on_core_shutdown: Callable[[], None] = lambda: None,
    ) -> None:
        self._core = core
        self._deps = deps
        self.base, self.ceiling, self._poll_ms, self._retire_ms, drain_ms = core.pool_open(
            os.cpu_count() or 1
        )
        self._drain_s = drain_ms / 1000
        # What a controller-initiated close reaches. Shutdown is observed three
        # ways — a signal, closed standard input, and the core's own stop — and
        # this is the third: the runner's own wait must end on it too.
        self._on_core_shutdown = on_core_shutdown
        self._pool = _Pool()
        self._stopping = False
        self._spawned = 0

    @property
    def name(self) -> str:
        """The name ``FLEZHA_PYTHON_EXECUTOR`` selects this member by."""
        raise NotImplementedError

    @property
    def size(self) -> int:
        """How many threads are alive now.

        Reported in the startup line and read by the pool-growth tests.
        """
        return self._pool.alive()

    def describe(self) -> str:
        """This host's conditions, for the runner's startup line.

        The executor knows its own sizing, so it says it — a caller reaching in
        for `base` and `ceiling` had to guess whether they were there.
        """
        return f"{self.name} executor, {self.size} threads, base {self.base} ceiling {self.ceiling}"

    def _start_pool(self) -> None:
        """The base workers in the seats the governor reserved, then its supervisor."""
        for _ in range(self.base):
            self._spawn(grown=False)
        self._pool.spawn("flezha-supervisor", self._supervise)

    def _supervise(self) -> None:
        """One governor answer per wait, at most a second apart, and the core's
        notices written on each, so an idle runner's notices still reach its log."""
        try:
            while not self._stopping:
                answer = self._core.pool_supervise()
                log_notices(self._core.take_notices())
                if answer == POOL_STOP or self._stopping:
                    return
                if answer in (POOL_SPAWN_BASE, POOL_SPAWN_GROWN):
                    self._spawn(grown=answer == POOL_SPAWN_GROWN)
        except Exception as error:  # noqa: BLE001 - the pool would stop growing
            _log.error("the pool supervisor ended on %s", error, exc_info=error)

    def _spawn(self, *, grown: bool) -> None:
        """One worker into a seat the governor reserved; a failed start gives it back."""
        self._spawned += 1
        try:
            self._spawn_worker(f"flezha-worker-{self._spawned}", grown=grown)
        except RuntimeError as error:
            self._core.pool_left()
            _log.warning("could not start a worker thread: %s", error)

    def _is_stopping(self) -> bool:
        return self._stopping

    def _spawn_worker(self, name: str, *, grown: bool) -> None:
        self._pool.spawn(
            name,
            lambda: _fetch_and_run(
                self._core,
                self._deps,
                self._is_stopping,
                poll_ms=self._poll_ms,
                retire_ms=self._retire_ms if grown else 0,
                on_core_shutdown=self._on_core_shutdown,
            ),
        )

    def stop_fetching(self) -> None:
        """Stop fetching without the core's word.

        A local run ends while the process-wide poll core keeps running —
        there is no controller session to shut down, and shutting the core
        would stop every later run in the process — so the flag is raised
        first and the workers leave on their next poll timeout instead of on
        the drain deadline.
        """
        self._stopping = True

    def stop(self) -> int:
        """Stop fetching and drain.

        The core says when: a worker leaves on the poll that reports the core
        shutting down, which the core reports only once the shutdown sequence
        has shipped every Cell's state through these same workers. A worker
        inside a handler finishes it, completes it, and only then observes it.
        Bounded by the governor's drain bound — a handler that never returns
        must not keep the runner alive forever — and the flag is raised after
        the join so a worker spawned late still leaves.
        """
        undrained = self._pool.join(time.monotonic() + self._drain_s)
        self._stopping = True
        _report_undrained(undrained, self._drain_s)
        log_notices(self._core.take_notices())
        return undrained


class ThreadExecutor(_Host):
    """The default: a base pool of threads each looping ``poll -> dispatch ->
    complete`` on itself, plus the governor's supervisor that grows it.

    This is the Rust host's model rather than Java's hand-off, and the reason is
    the reason behind both shipped hosts: Java hands off because a parked
    virtual thread costs a stack and nothing else — which is also exactly why
    Java's fetchers never wait on user code. A CPython thread is
    a real OS thread with a real stack, so hand-off would buy nothing and cost a
    thread per activation up to the core's in-flight cap.

    Concurrency is bounded by the core's ``FLEZHA_MAX_INFLIGHT_ACTIVATIONS``,
    never by the pool size. The pool is a servicing detail.
    """

    @property
    def name(self) -> str:
        return "thread"

    def start(self) -> ThreadExecutor:
        self._start_pool()
        return self


class AsyncioExecutor(_Host):
    """The opt-in executor for I/O-dense handlers: real-thread fetchers hand
    each activation to one shared event loop, which dispatches and completes on
    its own thread.

    The fetcher stays a real thread here too. ``poll`` blocks in native code,
    and ``docs/internal/sdk/poll_core.md`` is explicit that fetching on a runtime's
    shared I/O pool starves everything else that pool serves — so the loop's
    default executor is never borrowed for it.

    Completion happens on the loop thread, not on the fetcher's: the ABI
    documents a completion from a thread other than the poller's as expected,
    and the ordering guarantee is the core's handout rather than this
    executor's. ``uvloop`` is used when it is installed.
    """

    def __init__(
        self,
        core: Any,
        deps: DispatchDeps,
        *,
        loop_factory: Callable[[], asyncio.AbstractEventLoop] | None = None,
        **host: Any,
    ) -> None:
        super().__init__(core, deps, **host)
        self._loop_factory = loop_factory or _default_loop_factory
        self._loop: asyncio.AbstractEventLoop | None = None
        self._ready = threading.Event()
        self._room = threading.Semaphore(QUEUE_DEPTH)

    @property
    def name(self) -> str:
        return "asyncio"

    def start(self) -> AsyncioExecutor:
        self._pool.spawn("flezha-loop", self._run_loop)
        if not self._ready.wait(timeout=10.0):
            raise FlezhaError(
                "runtime.executor_unavailable",
                {"executor": "asyncio", "reason": "its event loop did not start within 10 s"},
            )
        self._start_pool()
        return self

    # -- internals ---------------------------------------------------------

    def _spawn_worker(self, name: str, *, grown: bool) -> None:
        """A fetcher, or for a grown seat a fetch-and-run thread: a stalled loop
        cannot rescue itself, so growing the loop's concurrency would rescue nothing."""
        if grown:
            super()._spawn_worker(name, grown=True)
        else:
            self._pool.spawn(name, self._run_fetcher)

    def _run_loop(self) -> None:
        loop = self._loop_factory()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._ready.set()
        try:
            loop.run_until_complete(self._serve())
        except Exception as error:  # noqa: BLE001 - the loop is the executor
            _log.error("the asyncio executor's loop ended on %s", error, exc_info=error)
        finally:
            loop.close()

    async def _serve(self) -> None:
        """Keep the loop alive until the drain, then let its dispatches finish.

        Closing the loop with tasks still running would abandon activations the
        core is holding cells for, so the drain waits for them the way the
        pool's join does — and is time-limited the same way.
        """
        while not self._stopping:
            await asyncio.sleep(self._poll_ms / 1000)
        pending = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
        if pending:
            await asyncio.wait(pending, timeout=self._drain_s)

    def _run_fetcher(self) -> None:
        def served(activation: Any) -> Any:
            if activation is None:
                return None
            self._room.acquire()
            loop = self._loop
            assert loop is not None
            asyncio.run_coroutine_threadsafe(self._dispatch(activation), loop)
            return None

        try:
            fetch(self._core, self._poll_ms, self._is_stopping, served, self._on_core_shutdown)
        except Exception as error:  # noqa: BLE001
            _log.error("a fetcher thread ended on %s", error, exc_info=error)
        finally:
            self._core.pool_left()

    async def _dispatch(self, activation: Any) -> None:
        try:
            await dispatch(activation, self._core, self._deps)
        finally:
            self._room.release()


def _default_loop_factory() -> asyncio.AbstractEventLoop:
    try:
        import uvloop  # type: ignore[import-not-found]
    except ImportError:
        return asyncio.new_event_loop()
    return uvloop.new_event_loop()


#: The executor seam's members, by the name ``FLEZHA_PYTHON_EXECUTOR`` selects.
EXECUTORS: dict[str, Callable[..., Any]] = {
    "thread": ThreadExecutor,
    "asyncio": AsyncioExecutor,
}


def build(kind: str | None, core: Any, deps: DispatchDeps, **options: Any) -> Executor:
    """The executor named by ``kind``, else by ``FLEZHA_PYTHON_EXECUTOR`` (``thread``
    by default), or a refusal listing the seam's members.

    Every member runs the governor's supervisor: there is no configuration that
    omits it, because a host whose threads are all parked in a completion has no
    other way back.
    """
    kind = kind or os.environ.get("FLEZHA_PYTHON_EXECUTOR", "thread")
    factory = EXECUTORS.get(kind)
    if factory is None:
        raise FlezhaError(
            "usage.unknown_word", {"noun": "executor", "value": kind, "allowed": sorted(EXECUTORS)}
        )
    return factory(core, deps, **options)
