"""The concurrency requirements, as behaviour rather than as configuration.

Every test here drives a real executor. The GIL-release cases drive the real
native library, because a fake core is Python and would prove nothing about the
lock.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

import flezha
from conftest import FakeCore, deps_for, solo_deps, work_activation
from flezha import executor as executor_module
from flezha import native
from flezha.dispatch import DispatchDeps
from flezha.errors import FlezhaError
from flezha.executor import AsyncioExecutor, ThreadExecutor, build


@flezha.tasklet(name="passthrough")
class Passthrough:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)


def _deps(core: FakeCore) -> DispatchDeps:
    return solo_deps(Passthrough, core)


def _until(predicate: Any, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.01)
    return False


# ---------------------------------------------------------------------------
# The pool: the core's governor decides, this host spawns
# ---------------------------------------------------------------------------


def test_the_pool_is_sized_by_the_core_over_this_hosts_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    core = native.Core.create()
    try:
        monkeypatch.setenv("FLEZHA_WORKER_POOL_SIZE", "5")
        assert core.pool_open(3)[:2] == [5, 50]
        monkeypatch.setenv("FLEZHA_WORKER_POOL_SIZE", "0")
        assert core.pool_open(3)[:2] == [3, 30], "the core answers unset"
        monkeypatch.setenv("FLEZHA_WORKER_POOL_MAX", "2")
        # A ceiling under the floor would retire the pool the user asked for.
        assert core.pool_open(4)[:2] == [4, 4]
    finally:
        core.free()


def test_a_workload_that_never_starves_runs_on_the_base_pool() -> None:
    core = FakeCore([work_activation(i, "x", factory="solo") for i in range(20)], base=3)
    pool = ThreadExecutor(core, _deps(core)).start()
    try:
        assert _until(lambda: len(core.completed) == 20)
        # The governor asked for nothing more, so nothing is grown.
        assert pool.size == 4  # three workers plus the supervisor
    finally:
        pool.stop()


def test_a_grown_seat_serves_and_retires_once_the_work_stops() -> None:
    release = threading.Event()

    @flezha.tasklet(name="blocks")
    class Blocks:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            release.wait(timeout=5)
            ctx.emit(value)

    @flezha.relay("blocking")
    def blocking(c: flezha.Channels) -> None:
        c.input("in").into(Blocks).output()

    core = FakeCore(
        [work_activation(i, "x", factory="blocking") for i in range(8)], base=1, retire_ms=500
    )
    pool = ThreadExecutor(core, deps_for(blocking, core)).start()
    try:
        for _ in range(3):
            core.decisions.put(executor_module.POOL_SPAWN_GROWN)
        assert _until(lambda: pool.size == 5), f"pool stayed at {pool.size}"
        release.set()
        assert _until(lambda: len(core.completed) == 8, timeout=10)
        # Two timed-out polls of the core's 250 ms are the 500 ms retire time.
        assert _until(lambda: pool.size == 2), f"pool stayed at {pool.size}"
        assert core.seats_left == 3, "every retired worker gives its seat back"
    finally:
        release.set()
        pool.stop()


def test_a_worker_that_dies_gives_its_seat_back_and_a_base_seat_replaces_it() -> None:
    class DiesOnce(FakeCore):
        died = False

        def poll(self, timeout_ms: int) -> Any:
            if not DiesOnce.died:
                DiesOnce.died = True
                raise RuntimeError("the worker's poll broke")
            return super().poll(timeout_ms)

    core = DiesOnce(base=1, retire_ms=200)
    pool = ThreadExecutor(core, _deps(core)).start()
    try:
        assert _until(lambda: core.seats_left == 1 and pool.size == 1)
        core.decisions.put(executor_module.POOL_SPAWN_BASE)
        assert _until(lambda: pool.size == 2), f"pool stayed at {pool.size}"
        core.offer(work_activation(1, "x", factory="solo"))
        assert _until(lambda: len(core.completed) == 1), "the replacement does not serve"
        # A base worker outlives the retire time a grown one leaves after.
        assert not _until(lambda: pool.size < 2, timeout=0.5)
    finally:
        pool.stop()


def test_an_idle_runner_writes_the_cores_notices_on_each_supervisor_answer(
    caplog: pytest.LogCaptureFixture,
) -> None:
    core = FakeCore(base=1)
    pool = ThreadExecutor(core, _deps(core)).start()
    try:
        with caplog.at_level("WARNING", logger="flezha.core"):
            core.notices.append(("warn", "flz_core::control_unit", "Connection attempt 2 failed"))
            assert _until(lambda: "Connection attempt 2 failed" in caplog.text, timeout=2)
    finally:
        pool.stop()


def test_a_grown_seat_rescues_a_block_policy_wedge_on_a_base_pool_of_one() -> None:
    """The case the supervisor exists for.

    A completion parks behind a full consumer mailbox; the only worker is
    inside it and is therefore not entering ``poll``, which is what the core
    reports as starvation. The worker the governor's grown seat starts is what
    drains the consumer and releases the producer.
    """
    drained = threading.Event()
    core = FakeCore(
        [
            work_activation(1, "producer", factory="solo"),
            work_activation(2, "consumer", factory="solo"),
        ],
        base=1,
    )
    # The first completion parks until a second worker gets far enough to
    # complete the second activation — which is exactly the wedge.
    gate = threading.Event()
    core.complete_gate = gate

    def release_when_a_second_thread_arrives() -> None:
        if _until(lambda: len(core.dispatch_threads) >= 2, timeout=5):
            drained.set()
        gate.set()

    watcher = threading.Thread(target=release_when_a_second_thread_arrives, daemon=True)
    watcher.start()

    pool = ThreadExecutor(core, _deps(core)).start()
    try:
        core.decisions.put(executor_module.POOL_SPAWN_GROWN)
        assert _until(lambda: drained.is_set(), timeout=10), "the grown seat bought no thread"
        assert _until(lambda: len(core.completed) == 2, timeout=10)
    finally:
        gate.set()
        pool.stop()
        watcher.join(timeout=2)


def test_the_supervisor_cannot_be_configured_away() -> None:
    """There is no executor without one; an unknown name is refused by name."""
    core = FakeCore()
    with pytest.raises(FlezhaError) as caught:
        build("single-threaded", core, _deps(core))
    assert (caught.value.code, caught.value.details["allowed"]) == (
        "usage.unknown_word",
        ["asyncio", "thread"],
    )
    assert sorted(executor_module.EXECUTORS) == ["asyncio", "thread"]


# ---------------------------------------------------------------------------
# Threads and ordering
# ---------------------------------------------------------------------------


def test_the_main_thread_is_never_a_dispatch_thread() -> None:
    """CPython runs signal handlers only on the main thread, between bytecodes."""
    core = FakeCore([work_activation(i, "x", factory="solo") for i in range(5)])
    pool = ThreadExecutor(core, _deps(core)).start()
    try:
        assert _until(lambda: len(core.completed) == 5)
        assert threading.main_thread().ident not in core.dispatch_threads
    finally:
        pool.stop()


@pytest.mark.parametrize("kind", ["thread", "asyncio"])
def test_ordering_is_identical_under_either_executor(kind: str) -> None:
    """An executor may affect throughput and latency; it may not affect order.

    With one activation outstanding at a time — which is what the core's
    per-cell handout guarantees — every executor serves the same sequence.
    """
    payloads = [f"v{i}" for i in range(12)]
    core = FakeCore()
    pool = build(kind, core, _deps(core)).start()
    try:
        for index, payload in enumerate(payloads):
            core.offer(work_activation(index, payload, factory="solo"))
            assert _until(lambda index=index: len(core.completed) == index + 1)
        served = [_payload_of(result) for result in core.completed]
        assert served == payloads
    finally:
        pool.stop()


def test_the_asyncio_executor_polls_on_a_real_thread_and_completes_on_the_loop() -> None:
    core = FakeCore([work_activation(i, "x", factory="solo") for i in range(4)])
    pool = AsyncioExecutor(core, _deps(core)).start()
    try:
        assert _until(lambda: len(core.completed) == 4)
        loop_threads = {
            thread.ident for thread in threading.enumerate() if thread.name == "flezha-loop"
        }
        # Every completion happened on the loop thread, and the fetchers — real
        # threads — are the ones that called poll.
        assert core.dispatch_threads <= loop_threads
        assert core.polls > 0
    finally:
        pool.stop()


@pytest.mark.parametrize("kind", ["thread", "asyncio"])
def test_an_async_handler_is_driven_under_either_executor(kind: str) -> None:
    import asyncio

    @flezha.tasklet(name="slow")
    class Slow:
        async def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            await asyncio.sleep(0.001)
            ctx.emit(value.upper())

    @flezha.relay("async_op")
    def async_op(c: flezha.Channels) -> None:
        c.input("in").into(Slow).output()

    core = FakeCore([work_activation(i, f"v{i}", factory="async_op") for i in range(4)])
    deps = deps_for(async_op, core)

    pool = build(kind, core, deps).start()
    try:
        assert _until(lambda: len(core.completed) == 4)
        assert sorted(_payload_of(r) for r in core.completed) == ["V0", "V1", "V2", "V3"]
    finally:
        pool.stop()


@pytest.mark.parametrize("kind", ["thread", "asyncio"])
def test_a_drain_has_a_time_limit_and_says_what_it_abandoned(
    kind: str, caplog: pytest.LogCaptureFixture
) -> None:
    """A handler that never returns must not keep the runner alive forever."""
    never = threading.Event()
    entered = threading.Event()

    @flezha.tasklet(name="wedged")
    class Wedged:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            entered.set()
            never.wait(timeout=30)
            ctx.emit(value)

    @flezha.relay("wedge")
    def wedge(c: flezha.Channels) -> None:
        c.input("in").into(Wedged).output()

    core = FakeCore([work_activation(1, "x", factory="wedge")], base=1, drain_ms=300)
    deps = deps_for(wedge, core)

    pool = build(kind, core, deps).start()
    try:
        assert entered.wait(timeout=5), "the handler never started"
        with caplog.at_level("WARNING", logger="flezha.executor"):
            started = time.monotonic()
            undrained = pool.stop()
            elapsed = time.monotonic() - started
        # The drain bound is the core's, here shortened through the policy.
        assert elapsed < 0.3 + 3
        assert undrained >= 1
        assert "still running after the 0.3s drain" in caplog.text
    finally:
        never.set()


def test_a_shutting_down_core_ends_every_worker() -> None:
    core = FakeCore()
    core.shut_down = True
    pool = ThreadExecutor(core, _deps(core)).start()
    try:
        assert _until(lambda: pool.size <= 1, timeout=5)
    finally:
        pool.stop()


# ---------------------------------------------------------------------------
# The GIL, against the real native library
# ---------------------------------------------------------------------------


def test_a_blocked_poll_does_not_freeze_the_interpreter() -> None:
    """Every worker parked in ``poll`` and an unrelated thread at full speed."""
    _assert_releases_the_gil(lambda core: core.poll(400))


def test_a_blocked_pool_supervisor_does_not_freeze_the_interpreter() -> None:
    _assert_releases_the_gil(lambda core: (core.pool_open(1), core.pool_supervise()))


def _assert_releases_the_gil(blocking_call: Any) -> None:
    """Run one blocking core call and count another thread's Python progress.

    Both blocking entry points go through the veneer's single ``py.detach``
    seam — which the crate's own test asserts is used exactly once per blocking
    export, so ``complete`` and the steps receive are covered by the same
    proof without needing a live controller to reach them.
    """
    core = native.Core.create()
    ticks = 0
    stop = threading.Event()

    def spin() -> None:
        nonlocal ticks
        while not stop.is_set():
            ticks += 1
            time.sleep(0)

    spinner = threading.Thread(target=spin, daemon=True)
    spinner.start()
    try:
        # Sample only once the spinner is running: its start-up would otherwise
        # be counted inside the blocked window it exists to measure.
        assert _until(lambda: ticks > 0), "the spinner never ran"
        before = ticks
        blocking_call(core)
        during = ticks - before
        assert during > 100, f"only {during} Python steps ran during a blocking core call"
    finally:
        stop.set()
        spinner.join(timeout=2)
        core.free()


def _payload_of(result: dict[str, Any]) -> Any:
    # The output names no codec, so a string output is its UTF-8 bytes.
    return bytes(result["effects"][0]["outputs"][0]["payload"]).decode()
