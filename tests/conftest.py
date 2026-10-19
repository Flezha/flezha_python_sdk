"""Shared fixtures: a fake core, and the declarations the tests drive.

The fake core answers the ABI's own vocabulary — ``poll`` gives an activation, a
timeout or ``"shutting_down"``, ``wait_starvation`` gives the three-way status —
so a test of the executor tests the executor rather than a mock of it.
"""

from __future__ import annotations

import inspect
import json
import queue
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pytest

import flezha
from flezha import native
from flezha.errors import FlezhaError

#: The process context ``graph.validate`` submits a record through. One per test
#: session: a check registers nothing, so every suite shares it.
CHECK_CORE = native.Core.create()


def declare(relay: flezha.Relay) -> dict[str, Any]:
    """Submit one relay's record, and answer what the core derived from it."""
    from flezha import graph

    return graph.validate(relay.spec, CHECK_CORE)


@flezha.state
@dataclass
class Counter:
    """The reference declared state: a scalar and a list."""

    count: int = 0
    seen: list[str] = field(default_factory=list)


@dataclass
class Bumped:
    """The reference declared change."""

    by: int


class CountUp:
    """A stateful tasklet: its state is the context's, built by `init_state`."""

    def run(self, value: str, ctx: flezha.Ctx[Counter, dict[str, int]]) -> None:
        ctx.change(Bumped(by=len(value)))
        ctx.emit({"total": ctx.state.count})

    def init_state(self, key: str | None) -> Counter:
        return Counter()

    def apply_change(self, state: Counter, change: Bumped) -> None:
        state.count += change.by
        state.seen.append(str(change.by))


count_up = flezha.tasklet(CountUp, name="count_up")


class Echo:
    """A stateless tasklet: what it is handed, it hands on."""

    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)


echo = flezha.tasklet(Echo, name="echo")


@flezha.relay("demo")
def demo(c: flezha.Channels) -> None:
    c.input("input").into(echo).named("mid").into(count_up).output("out")


class FakeCore:
    """A core that answers the ABI's vocabulary, with no native library.

    Every executor test drives this: what it proves is that the host reads
    ``poll`` and ``wait_starvation`` the way the ABI defines them.
    """

    def __init__(
        self,
        activations: list[Any] | None = None,
        *,
        starve_after: float | None = None,
    ) -> None:
        self._activations: queue.Queue[Any] = queue.Queue()
        for activation in activations or []:
            self._activations.put(activation)
        self._lock = threading.Lock()
        self._starve_after = starve_after
        self._shutting = threading.Event()
        self.completed: list[dict[str, Any]] = []
        self.abandoned: list[int] = []
        self.polls = 0
        self.pollers_inside = 0
        self.peak_pollers = 0
        self.shut_down = False
        self.freed = False
        self.declared: list[bytes] = []
        self.started: str | None = None
        self.complete_gate: threading.Event | None = None
        self.dispatch_threads: set[int] = set()
        self.stdin_watched = False
        self.undeclared: list[str] = []
        self.spawns: list[dict[str, Any]] = []
        #: What a spawn resolves to beside its id: an outcome, or the error a
        #: refused read raises. ``None`` spawns under the requested or a
        #: generated id.
        self.spawn_answer: dict[str, str] | FlezhaError | None = None
        #: How long after its request a spawn is answered, as a controller takes.
        self.spawn_answer_after_s = 0.0
        #: The relay instances the core reports bound.
        self.bound: list[str] = []

    # -- the ABI ----------------------------------------------------------

    def declare_class(self, json_bytes: bytes) -> None:
        self.declared.append(json_bytes)

    def undeclare_class(self, factory_id: str) -> None:
        self.undeclared.append(factory_id)

    def check_class(self, json_bytes: bytes) -> bytes:
        # The one entry point the fake does not fake: a check registers nothing
        # and mutates nothing, so the real core is what a test wants an answer
        # from — a fake verdict would only prove the fake.
        return CHECK_CORE.check_class(json_bytes)

    def start(self, namespace: str) -> None:
        self.started = namespace

    def offer(self, activation: Any) -> None:
        """Queue one activation for the next poller, as a controller would."""
        self._activations.put(activation)

    def poll(self, timeout_ms: int) -> Any:
        with self._lock:
            self.polls += 1
            self.pollers_inside += 1
            self.peak_pollers = max(self.peak_pollers, self.pollers_inside)
            shutting = self.shut_down
        try:
            if shutting:
                return "shutting_down"
            try:
                return self._activations.get(timeout=min(timeout_ms, 20) / 1000)
            except queue.Empty:
                return None
        finally:
            with self._lock:
                self.pollers_inside -= 1

    def take_notices(self) -> list[tuple[str, str, str]]:
        return []

    def wait_starvation(self, timeout_ms: int) -> int:
        """The core's condition, not an unconditional yes.

        Starvation is *work queued with no thread inside poll*. A fake that
        answered `observed` forever would make the supervisor grow, retire and
        grow again in perpetuity — which is a defect of the fake, and would make
        a retirement assertion true only inside a window.
        """
        if self.shut_down:
            return 2
        waited = timeout_ms / 1000 if self._starve_after is None else self._starve_after
        if self._shutting.wait(timeout=waited):
            return 2
        if self._starve_after is None:
            return 0
        return 1 if not self._activations.empty() else 0

    def complete(self, result: dict[str, Any]) -> None:
        self.dispatch_threads.add(threading.get_ident())
        if self.complete_gate is not None:
            self.complete_gate.wait(timeout=5)
        with self._lock:
            self.completed.append(result)

    def abandon(self, token: int) -> None:
        with self._lock:
            self.abandoned.append(token)

    def shutdown(self) -> None:
        self.shut_down = True
        self._shutting.set()

    def watch_stdin_eof(self) -> None:
        self.stdin_watched = True

    def relays(self) -> list[str]:
        return list(self.bound)

    def free(self) -> None:
        self.freed = True

    def spawn_request(
        self,
        token: int,
        factory_id: str,
        id: str,
        payload: bytes,
        if_running: str,
        replay: bool,
        offset: int | None,
    ) -> int:
        if if_running and not id:
            raise FlezhaError("spawn.if_running_without_id", {})
        with self._lock:
            self.spawns.append(
                {
                    "token": token,
                    "factory_id": factory_id,
                    "id": id,
                    "payload": payload,
                    "if_running": if_running,
                    "replay": replay,
                    "offset": offset,
                    "due": time.monotonic() + self.spawn_answer_after_s,
                }
            )
            return len(self.spawns)

    def spawn_result(self, handle: int, timeout_ms: int) -> str | None:
        """The core's read: a replay resolves at once and sends nothing, and
        anything else resolves once its answer is due."""
        request = self.spawns[handle - 1]
        if request["replay"]:
            return json.dumps({"id": request["id"], "outcome": "skipped_on_replay"})
        left = request["due"] - time.monotonic()
        if left > timeout_ms / 1000:
            return None
        time.sleep(max(left, 0.0))
        answer = self.spawn_answer or {"outcome": "spawned"}
        if isinstance(answer, FlezhaError):
            raise answer
        generated = f"{request['factory_id']}-k7m2qzxw4pd3fn2a"
        return json.dumps({"id": request["id"] or generated, **answer})


def codes(error: FlezhaError) -> list[str]:
    """Every code a refusal carries: each issue's, or its own."""
    return [issue["code"] for issue in error.details.get("issues", [])] or [error.code]


def _unscripted(entry: str) -> Callable[..., None]:
    """An entry point no test scripts: it takes what the wrapper takes and
    answers ``None``, so a call the core would refuse for its shape fails here
    too."""
    signature = inspect.signature(getattr(native.Core, entry))

    def call(self: FakeCore, *args: Any, **kwargs: Any) -> None:
        signature.bind(self, *args, **kwargs)

    call.__name__ = entry
    return call


# Generated from the wrapper's own list, so an entry point the veneer gains
# reaches the double too. A test that needs a scripted answer defines the method
# in the class body, which this skips.
for _entry in set(native.Core.ENTRY_POINTS) - set(vars(FakeCore)):
    setattr(FakeCore, _entry, _unscripted(_entry))


def work_activation(
    token: int, *payloads: Any, factory: str = "demo", tasklet: int = 0
) -> dict[str, Any]:
    """One work activation, as the veneer produces it.

    Variadic because a batch is the interesting case — a failure mid-batch stops
    it — and a one-payload call is the same builder with one argument. Each
    payload is JSON, which is what an entry naming no codec reads.
    """
    return {
        "token": token,
        "relay": "op",
        "tasklet": tasklet,
        "change_seq": 0,
        "factory": factory,
        "kind": "work",
        "items": [
            {
                "kind": "input",
                "channel": 0,
                "payload": json.dumps(payload).encode(),
                "metadata": [],
                "consumed_offset": token + index,
                "replay": False,
                "now": 0,
            }
            for index, payload in enumerate(payloads)
        ],
    }


def wire_work(
    token: int,
    factory: str,
    *items: tuple[int, bytes],
    tasklet: int = 0,
    cell: str | None = None,
) -> dict[str, Any]:
    """One work activation over wire payloads, ``(channel, payload)`` per item."""
    activation = work_activation(token, *[None] * len(items), factory=factory, tasklet=tasklet)
    activation["cell_key"] = cell
    for item, (channel, payload) in zip(activation["items"], items, strict=True):
        item.update(channel=channel, payload=payload)
    return activation


def key_request(
    token: int,
    factory: str,
    *payloads: bytes,
    channel: int = 0,
    want_keys: bool = True,
    want_event_time: bool = False,
) -> dict[str, Any]:
    """One key request over wire payloads, as the veneer produces it."""
    return {
        "token": token,
        "relay": "op",
        "tasklet": 0,
        "change_seq": 0,
        "factory": factory,
        "kind": "key_request",
        "key_request": {
            "channel": channel,
            "payloads": list(payloads),
            "want_keys": want_keys,
            "want_event_time": want_event_time,
        },
    }


def leave(token: int, *, whole_relay: bool, tasklet: int = 0) -> dict[str, Any]:
    """A leave for relay ``op``, as the veneer produces it."""
    return {
        "token": token,
        "relay": "op",
        "tasklet": tasklet,
        "change_seq": 0,
        "factory": "demo",
        "kind": "leave",
        "whole_relay": whole_relay,
    }


def answered(activation: dict[str, Any], deps: Any) -> dict[str, Any]:
    """The answer for one activation, driven with no event loop."""
    from flezha.dispatch import answer, run_now

    return run_now(answer(activation, deps))


def deps_for(relay: flezha.Relay, core: FakeCore | native.Core) -> Any:
    """A validated, registered `DispatchDeps` over one relay.

    Twelve tests across two files spelled `Registry() + validate + add +
    DispatchDeps` out longhand; this is that, once, so a change to what a
    dispatch loop needs is one edit.
    """
    from flezha.dispatch import DispatchDeps, Registry

    registry = Registry()
    declare(relay)
    registry.add(relay.spec)
    return DispatchDeps(registry=registry, bridge=core)


def compensating(seen: list[Any]) -> Any:
    """A tasklet whose compensation appends the outputs it is driven over.

    Two suites drive the same shape — the dispatcher's `compensate_run` arm and
    the harness's `compensate` — so the tasklet is declared once.
    """

    class Compensating:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

        def compensate(self, outputs: list[Any], ctx: flezha.Ctx[None, str]) -> None:
            seen.extend(outputs)

    return flezha.tasklet(Compensating, name="compensating")


def solo(task: flezha.Tasklet[Any, Any], factory: str = "solo") -> flezha.Relay:
    """A one-tasklet relay wrapped around `task`, its output on an output."""

    @flezha.relay(factory)
    def relay(c: flezha.Channels) -> None:
        c.input("in").into(task).output()

    return relay


def solo_deps(task: flezha.Tasklet[Any, Any], core: FakeCore, factory: str = "solo") -> Any:
    """`deps_for` over `solo(task)`."""
    return deps_for(solo(task, factory), core)


def pytest_configure(config: pytest.Config) -> None:
    """Fail any test that trips a determinism warning.

    Declared here and not in ``pyproject.toml``: pytest resolves an ini-time
    category by importing its package, which happens before coverage starts
    tracing and leaves every module executed at import unmeasured.
    """
    config.addinivalue_line("filterwarnings", "error::flezha.DeterminismWarning")


@pytest.fixture
def fake_core() -> FakeCore:
    return FakeCore()


@pytest.fixture(autouse=True)
def _quiet_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep the executor tests off the machine's processor count."""
    monkeypatch.setenv("FLEZHA_WORKER_POOL_SIZE", "2")
    monkeypatch.delenv("FLEZHA_WORKER_POOL_MAX", raising=False)
