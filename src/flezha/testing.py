"""Driving one tasklet without a controller, for the author's own tests.

The harness is not a second implementation of the dispatcher: it drives
:func:`flezha.dispatch.invoke` — the same function every activation kind that
reaches a handler goes through — over a one-tasklet
:class:`~flezha.dispatch.RegisteredClass`, and :meth:`TaskletTestHarness.replay_equals`
folds through :func:`flezha.cell_state.fold_restore`, the function a real restore
calls. So what it proves is what the runner does, rather than something that
agrees with it.
"""

from __future__ import annotations

import base64
import json
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

from . import codec as _codec
from .builder import Channels, ClassSpec
from .cell_state import fold_restore
from .dispatch import DispatchDeps, RegisteredClass, invoke, run_now
from .errors import FlezhaError
from .tasklet import Tasklet

__all__ = [
    "TaskletTestHarness",
    "ReplayMismatch",
    "EffectReplayMismatch",
    "Outcome",
    "replay_equals",
]

#: The longest effect name, in bytes, as the core refuses it.
_MAX_EFFECT_NAME = 255

S = TypeVar("S")


class ReplayMismatch(AssertionError):
    """The recorded change log and the live state disagree: the harness's own
    assertion about the tasklet under test, not a refusal."""

    def __init__(self, tasklet: str, live: Any, replayed: Any) -> None:
        self.live = live
        self.replayed = replayed
        super().__init__(
            f"{tasklet}: replaying the recorded change log against a fresh initial state "
            f"produced {replayed!r}, but the live state is {live!r}. Either a handler mutated "
            f"the state without recording a change, or apply_change is not a pure fold — both "
            f"are lost on restart and reported nowhere until then."
        )


class EffectReplayMismatch(AssertionError):
    """A replayed input made an outside call its live run kept through
    ``ctx.effect`` some other way: its function ran again, or the kept result
    was never asked for."""

    def __init__(self, tasklet: str, name: str, what: str) -> None:
        self.name = name
        super().__init__(
            f"{tasklet}: on the replay, effect '{name}' {what}. A re-run reads an outside "
            f"call's result only through ctx.effect, with the same name in the same order."
        )


@dataclass(slots=True)
class _Effects:
    """One input's effect results, by ``(name, ordinal)``, and the invocation's
    count of each name, the call whose function is running, and what a replay
    asked for."""

    kept: dict[tuple[str, int], bytes] = field(default_factory=dict)
    replaying: bool = False
    ordinals: dict[str, int] = field(default_factory=dict)
    running: tuple[str, int] | None = None
    asked: set[tuple[str, int]] = field(default_factory=set)


@dataclass(slots=True)
class Outcome:
    """What one invocation produced."""

    outputs: list[Any] = field(default_factory=list)
    changes: list[Any] = field(default_factory=list)
    timers: list[dict[str, Any]] = field(default_factory=list)


class TaskletTestHarness(Generic[S]):
    """Drive one tasklet the way a runner does, with no controller running.

    ``async def`` handlers are driven exactly as the thread executor drives
    them, so a test needs no event loop of its own.

    A ``ctx.effect`` keeps its result on a live :meth:`feed`; the n-th
    ``feed(..., replay=True)`` replays the n-th live input and is served what it
    kept, and raises :class:`EffectReplayMismatch` when a function runs there or
    a kept result is never asked for.
    """

    def __init__(
        self,
        task: Tasklet[Any, Any],
        *,
        cell_key: str | None = None,
        relay: str = "harness",
        initial_state: S | None = None,
    ) -> None:
        self._task = task
        self._cell_key = cell_key
        self._relay = relay
        # The real channel builder, not a hand-written spec: one entry channel
        # into the tasklet and one plugin output so `ctx.emit` has somewhere to
        # go — a tasklet under test emits to be observed, not to be routed.
        # Driving the builder means the harness proves it too. The output is
        # MessagePack so `outputs` reports exactly the value a handler emitted.
        channels = Channels("<harness>")
        channels.input("in").into(task).output(codec="msgpack")
        self._spec = ClassSpec(
            factory_id="<harness>",
            tasklets=channels.tasklets,
            channels=channels.channels,
        )
        self._node = channels.tasklets[0]
        # The dispatcher's own cell store, so the state this harness reports
        # is the state a runner would hold.
        self._registered = RegisteredClass(self._spec)
        # No core runs here, so the harness names the cell's identity itself.
        self._cell = self._registered.cell(self._node, relay, cell_key)
        self._registered.keep_identity(self._cell, f"{relay}/{task.name}/{cell_key}".encode())
        self._deps = DispatchDeps(registry=None, bridge=self)
        self.log: list[bytes] = []
        # Pending timers by (kind, scheduled_at), as the core keeps them.
        self._timers: set[tuple[str, int]] = set()
        self._change_seq = 0
        # Answers by handle, until the invocation that made them ends, as the
        # core releases them.
        self._spawns: dict[int, str] = {}
        self._spawn_count = 0
        # Each live input's effect results, in order, and how many replays ran.
        self._kept_effects: list[dict[tuple[str, int], bytes]] = []
        self._replays = 0
        self._effects = _Effects()
        if initial_state is not None:
            self._cell.state = initial_state

    @property
    def state(self) -> S:
        """The cell's live state, as the dispatcher left it."""
        return self._cell.state

    def feed(self, value: Any, *, now: int = 0, replay: bool = False) -> Outcome:
        """Run the tasklet against one input; ``replay`` replays the next live
        input, in order."""
        if replay:
            index = self._replays
            self._replays += 1
            kept = self._kept_effects[index] if index < len(self._kept_effects) else {}
            self._effects = _Effects(kept=kept, replaying=True)
        else:
            self._effects = _Effects()
            self._kept_effects.append(self._effects.kept)
        outcome = self._run(now, replay, lambda ctx: self._task(value, ctx), "run")
        unasked = [key for key in self._effects.kept if key not in self._effects.asked]
        if replay and unasked:
            raise EffectReplayMismatch(self._task.name, unasked[0][0], "was never asked for")
        return outcome

    def apply_inputs(self, values: Iterable[Any], *, now: int = 0) -> list[Outcome]:
        """Run the tasklet against several inputs, in order."""
        return [self.feed(value, now=now) for value in values]

    def fire_timer(self, kind: str, scheduled_at: int) -> Outcome:
        """Fire a per-cell timer. ``now`` IS the scheduled time, as it is live."""
        if self._task.on_timer is None:
            raise FlezhaError("runtime.timer_without_on_timer", {"tasklet": self._task.name})
        self._timers.discard((kind, scheduled_at))
        return self._run(
            scheduled_at,
            False,
            lambda ctx: self._task.on_timer(  # type: ignore[misc]
                {"kind": kind, "scheduled_at": scheduled_at}, ctx
            ),
            "on_timer",
        )

    def advance_event_time(self, to: int) -> list[Outcome]:
        """Fire each pending event-time timer at or before ``to``, earliest
        first; what a firing registers or cancels counts, as it does live."""
        outcomes: list[Outcome] = []
        while due := [at for kind, at in self._timers if kind == "event_time" and at <= to]:
            outcomes.append(self.fire_timer("event_time", min(due)))
        return outcomes

    def compensate(self, outputs: list[Any]) -> Outcome:
        """Run the declared compensation over recorded forward outputs."""
        if self._task.compensate is None:
            raise FlezhaError(
                "runtime.hook_missing",
                {
                    "tasklet": self._task.name,
                    "action": "was asked to compensate",
                    "hook": "compensate",
                },
            )
        return self._run(
            0,
            False,
            lambda ctx: self._task.compensate(outputs, ctx),  # type: ignore[misc]
            "compensate",
        )

    def restore(self, *, snapshot: bytes | None = None, changes: Iterable[bytes] = ()) -> S:
        """Rebuild the state the way a restore does, from a snapshot and a log.

        The same call the dispatcher answers ``restore_fold`` with.
        """
        state = fold_restore(
            self._task,
            {
                "snapshot": {"bytes": snapshot} if snapshot is not None else None,
                "changes": [{"bytes": body} for body in changes],
            },
            self._cell_key,
            self._task.name,
        )
        self._cell.state = state
        return state

    def snapshot(self) -> bytes:
        """The state as a snapshot body, encoded the way the dispatcher does."""
        return self._task.encode_state(self.state)

    def replay_equals(self) -> bool:
        """Fold every recorded change into a fresh initial state and require it
        to equal the live one.

        In this SDK that catches both failures at once: a handler that mutated
        the state without recording a change — which the state guard refuses
        for a declared class but not for a plain dict — and an ``apply_change``
        that is not a pure fold.
        """
        replayed = fold_restore(
            self._task,
            {"snapshot": None, "changes": [{"bytes": body} for body in self.log]},
            self._cell_key,
            self._task.name,
        )
        live = self.state
        if replayed != live:
            raise ReplayMismatch(self._task.name, live, replayed)
        return True

    # -- the core bridge a `Ctx` asks ----------------------------------------

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
        """A harness has no controller, so a spawn resolves at once to what one
        would answer for a free id: ``spawned`` under the given id, or under a
        deterministic one shaped like a generated id. A replay answers
        ``skipped_on_replay`` with the given id, or an empty one."""
        del token, payload, if_running, offset
        self._spawn_count += 1
        if replay:
            answer = {"id": id, "outcome": "skipped_on_replay"}
        else:
            suffix = base64.b32encode(self._spawn_count.to_bytes(10, "big")).decode().lower()
            answer = {"id": id or f"{factory_id}-{suffix}", "outcome": "spawned"}
        self._spawns[self._spawn_count] = json.dumps(answer)
        return self._spawn_count

    def spawn_result(self, handle: int, timeout_ms: int) -> str | None:
        del timeout_ms
        answer = self._spawns.get(handle)
        if answer is None:
            raise FlezhaError("runtime.spawn_handle_released", {"handle": handle})
        return answer

    def part(self, token: int, item: int, payload: bytes) -> None:
        """A harness has no controller, so no listener: the part, already
        encoded by the context, is dropped."""
        del token, item, payload

    def effect_begin(self, token: int, item: int, name: str) -> tuple[str, bytes | None]:
        """The core's answer to a begun ``ctx.effect``, over this input's
        results: the kept one, or run the function (on a live input only)."""
        del token, item
        if not name or len(name.encode()) > _MAX_EFFECT_NAME:
            raise FlezhaError("effect.bad_name", {"name": repr(name)})
        effects = self._effects
        if effects.running is not None:
            raise FlezhaError("effect.nested", {"name": name})
        ordinal = effects.ordinals.get(name, 0)
        effects.ordinals[name] = ordinal + 1
        key = (name, ordinal)
        effects.asked.add(key)
        if key in effects.kept:
            return "kept", effects.kept[key]
        if effects.replaying:
            raise EffectReplayMismatch(self._task.name, name, "ran its function")
        effects.running = key
        return "run", None

    def effect_end(self, token: int, item: int, payload: bytes) -> bytes | None:
        """Keep a live input's result, as the controller does."""
        del token, item
        effects = self._effects
        assert effects.running is not None, "an end follows a begin that runs"
        effects.kept[effects.running] = payload
        effects.running = None
        return None

    def effect_release(self, token: int, item: int) -> None:
        del token, item
        self._effects.running = None

    # -- internals -----------------------------------------------------------

    def _run(self, now: int, replay: bool, body: Any, hook: str) -> Outcome:
        activation = {
            "token": 0,
            "relay": self._relay,
            "cell_key": self._cell_key,
            "change_seq": self._change_seq,
        }
        try:
            ctx, returned = run_now(invoke(self._cell, activation, self._deps, now, replay, body))
        finally:
            self._spawns.clear()
        ctx.settle_return(returned, hook)
        self.log.extend(ctx.changes)
        self._change_seq += len(ctx.changes)
        for timer in ctx.timers:
            apply = self._timers.add if timer["register"] else self._timers.discard
            apply((timer["kind"], timer["scheduled_at"]))
        return Outcome(
            outputs=[_codec.decode(output["payload"]) for output in ctx.outputs],
            changes=[self._task.decode_change(body) for body in ctx.changes],
            timers=list(ctx.timers),
        )


def replay_equals(
    task: Tasklet[Any, Any], inputs: Iterable[Any], *, cell_key: str | None = None
) -> bool:
    """Drive ``task`` over ``inputs`` and assert the replay agrees."""
    harness: TaskletTestHarness[Any] = TaskletTestHarness(task, cell_key=cell_key)
    harness.apply_inputs(inputs)
    return harness.replay_equals()
