"""The invocation context: every deterministic effect a handler is allowed.

The context accumulates one item's effects — recorded changes, outputs, timer
operations, the checkpoint flag — and the dispatcher turns them into the item
effect the core folds. Nothing here reaches the wire on its own; an emit is a
value in a list until the completion carries it.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Generator, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from . import codec as _codec
from .builder import ChannelSpec, ClassSpec, ConsumerEdge, ProcNode, apply_transforms
from .errors import EmitError, SpawnError
from .identity import entity_identity, stable_id

if TYPE_CHECKING:
    from .native import Core

__all__ = [
    "Ctx",
    "SpawnHandle",
    "SpawnResult",
    "SpawnOutcome",
    "IfRunning",
    "RESERVED_PREFIX",
]

S = TypeVar("S")

#: Metadata keys the runtime owns. A handler that sets one is refused.
RESERVED_PREFIX = "flz."

#: The identity stamps a non-tier keyed plugin emit carries.
#:
#: Under the effectively-once tier the controller derives them at flush time
#: from the effect record, so none ride on the frame; off the tier the frame is
#: written straight to the wire and the stamps have nowhere else to come from.
#: Byte-shape and condition both mirror the Java host, because a sink
#: deduplicates on them and two SDKs disagreeing about the shape is a sink that
#: silently stops deduplicating.
ENTITY_STAMP = "flz.entity"
SEQ_STAMP = "flz.seq"
KEY_STAMP = "flz.key"

#: What a spawn with an id does when that id is already running.
IfRunning = Literal["deliver", "error"]

#: How a spawn reached its instance.
SpawnOutcome = Literal["spawned", "delivered", "skipped_on_replay"]

#: The waiting read's own bound: none. The core ends every wait at the
#: request's deadline, 30 seconds after it was made.
_UNTIL_DEADLINE_MS = 2**64 - 1


@dataclass(frozen=True, slots=True)
class SpawnResult:
    """A spawn that reached an instance: the id it resolved to, the caller's or
    the generated one, and how. A replayed spawn without an id answers an empty
    id, because the generated one exists only in the live run's answer."""

    id: str
    outcome: SpawnOutcome


class SpawnHandle:
    """The answer to one ``ctx.spawn_workflow``, read inside the invocation that
    made it.

    ``wait()`` blocks for the answer; ``try_result()`` never waits and answers
    ``None`` while it is pending. Either raises a :class:`~flezha.errors.SpawnError`
    subclass when the spawn reached no instance. A resolved handle answers every
    read the same until the invocation ends, which releases it: a later read
    raises :class:`~flezha.errors.CoreError`, and a failure no read collected is
    logged once by the core. In an ``async def`` handler, ``await handle`` waits
    on the event loop's default executor, so the loop keeps serving.
    """

    __slots__ = ("_bridge", "_handle")

    def __init__(self, bridge: Core | Any, handle: int) -> None:
        self._bridge = bridge
        self._handle = handle

    def wait(self) -> SpawnResult:
        """Block for the answer, up to the core's deadline for the request."""
        result = self._read(_UNTIL_DEADLINE_MS)
        assert result is not None, "a read that reaches the deadline resolves to a timeout"
        return result

    def try_result(self) -> SpawnResult | None:
        """The answer if it has arrived, else ``None``. Never waits."""
        return self._read(0)

    def __await__(self) -> Generator[Any, None, SpawnResult]:
        return asyncio.to_thread(self.wait).__await__()

    def _read(self, timeout_ms: int) -> SpawnResult | None:
        answer = self._bridge.spawn_result(self._handle, timeout_ms)
        if answer is None:
            return None
        fields = json.loads(answer)
        if "error" in fields:
            raise SpawnError.of(fields)
        return SpawnResult(fields["id"], fields["outcome"])


class Ctx(Generic[S]):
    """What a handler is handed alongside its input."""

    __slots__ = (
        "_spec",
        "_node",
        "_outgoing",
        "_operative",
        "_change_seq",
        "_now",
        "_bridge",
        "_token",
        "_identity",
        "_ordinal",
        "_emit_ordinal",
        "changes",
        "outputs",
        "timers",
        "entity_key",
        "replay",
        "state",
        "checkpointed",
        "completion",
    )

    def __init__(
        self,
        *,
        spec: ClassSpec,
        node: ProcNode,
        outgoing: list[ChannelSpec],
        operative: str,
        entity_key: str | None,
        change_seq: int,
        now: int,
        replay: bool,
        state: S,
        bridge: Core | Any,
        token: int,
    ) -> None:
        self._spec = spec
        self._node = node
        self._outgoing = outgoing
        self._operative = operative
        self._change_seq = change_seq
        self._now = now
        self._bridge = bridge
        self._token = token
        self._identity: bytes | None = None
        self._ordinal = 0
        # Separate from the `stable_id` ordinal on purpose: one counts
        # identifiers a handler asked for, the other counts frames it produced,
        # and a handler that does one and not the other must not shift the
        # sequence the sink deduplicates on.
        self._emit_ordinal = 0
        self.entity_key = entity_key
        self.replay = replay
        #: The entity's state. Mutated only through :meth:`change`: a field
        #: assigned directly is live in this process and gone after a restart,
        #: which is what the state guard refuses.
        self.state = state
        self.changes: list[bytes] = []
        self.outputs: list[dict[str, Any]] = []
        self.timers: list[dict[str, Any]] = []
        self.checkpointed = False
        self.completion: bytes | None = None

    # -- deterministic reads ----------------------------------------------

    def now(self) -> int:
        """The core-evaluated declared clock for this item. Never ambient time."""
        return self._now

    def stable_id(self) -> str:
        """A replay-stable identifier, distinct per call within the invocation."""
        if self._identity is None:
            # Lazily, because most invocations never ask and the identity is
            # the one part of the derivation that costs an allocation.
            self._identity = entity_identity(self._operative, self._node.name, self.entity_key)
        answer = stable_id(self._identity, self._change_seq, self._ordinal)
        self._ordinal += 1
        return answer

    # -- effects ------------------------------------------------------------

    def change(self, change: Any) -> None:
        """Record one declared change: journaled, then folded into the state."""
        proc = self._node.proc
        if proc.apply_change is None:
            raise EmitError(
                f"{self._node.name}: recorded a change but declares no apply_change; a change "
                f"that cannot be folded cannot rebuild the state it describes"
            )
        self.changes.append(proc.encode_change(change))
        # Fold it live through the same path the restore fold uses, so the live
        # path and the replay path cannot drift.
        proc.fold_change(self.state, change)

    def emit(
        self,
        value: Any,
        *,
        key: str | None = None,
        channel: str | None = None,
        metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None = None,
    ) -> None:
        """Emit one output value on the procedure's declared output.

        ``channel`` selects one when the procedure produces on several.
        """
        targets = [c for c in self._outgoing if c.name == channel] if channel else self._outgoing
        if not targets:
            raise EmitError(
                f"{self._node.name}: emits on channel `{channel}`, which it does not produce on"
                if channel
                else f"{self._node.name}: emits but declares no output channel; wire its edge "
                f"with into(...) or sink(...)"
            )
        for target in targets:
            self._release(target, value, key, metadata)

    def timer_after(self, milliseconds: int) -> int:
        """Register a processing-time timer ``milliseconds`` from ``now``.

        Anchored on ``now`` and never on an ambient read: that is what makes a
        replayed invocation register the timestamp the live one did.
        """
        at = self._now + int(milliseconds)
        self.timers.append({"kind": "processing_time", "scheduled_at": at, "register": True})
        return at

    def timer_at_event_time(self, at: int) -> int:
        """Register an event-time timer at an absolute time."""
        at = int(at)
        self.timers.append({"kind": "event_time", "scheduled_at": at, "register": True})
        return at

    def cancel_timer(self, kind: str, at: int) -> None:
        """Cancel a registered timer by the value its registration answered."""
        self.timers.append({"kind": kind, "scheduled_at": int(at), "register": False})

    def checkpoint(self) -> None:
        """Close the open checkpoint interval (checkpoint ack-granularity classes)."""
        self.checkpointed = True

    def complete(self, result: Any = None) -> None:
        """End the workflow instance this procedure runs in, with ``result``.

        The rest of the iteration still runs and commits; the instance then
        ends with status ``ok`` instead of waiting for its next input. In a
        ``once`` workflow it sets the result the pass ends with. ``result`` is
        encoded as a sink naming no codec encodes a value: bytes as they are, a
        string as UTF-8 and anything else as JSON; ``None`` carries no result. A
        later call in the same invocation replaces an earlier one.

        A stream never completes: a stream procedure calling this fails its
        input, without a retry, with an error naming ``CompleteOutsideWorkflow``.
        """
        self.completion = b"" if result is None else _codec.plain_payload(result, self._node.name)

    def spawn_workflow(
        self,
        factory_id: str,
        payload: Any,
        *,
        id: str | None = None,
        if_running: IfRunning | None = None,
    ) -> SpawnHandle:
        """Start a workflow instance of ``factory_id`` with ``payload`` as its
        first input, and answer a :class:`SpawnHandle` at once.

        Without ``id`` the controller generates one and always starts a new
        instance: it can never deliver into a running one, a reissue after a
        lost or timed-out answer starts a second instance, and a replay answers
        an empty id. A caller who needs at most one instance, or the id on
        replay, passes its own ``id``. With one, ``if_running`` decides what a
        running instance under it gets: ``"deliver"`` (the default) feeds it
        ``payload``, ``"error"`` resolves the handle to
        :class:`~flezha.errors.SpawnAlreadyRunningError`.

        Never waits and never raises a controller refusal: every answer comes
        through the handle. A replayed invocation sends nothing and resolves to
        ``skipped_on_replay``, where a timer registers again, because a spawn is
        an external side effect and a timer is internal state.
        """
        if if_running is not None and not id:
            raise ValueError(
                f"{self._node.name}: if_running applies only to a running instance named by "
                f"id; pass id= with if_running, or drop if_running to start a new instance"
            )
        return SpawnHandle(
            self._bridge,
            self._bridge.spawn_request(
                self._token,
                factory_id,
                id or "",
                _codec.encode(payload),
                if_running or "",
                self.replay,
            ),
        )

    # -- what the dispatcher collects ---------------------------------------

    def effect(self, consumed_offset: int | None) -> dict[str, Any]:
        """The effect record for this item, as the core folds it."""
        return {
            "verdict": "ok",
            "changes": self.changes,
            "outputs": self.outputs,
            "consumed_offset": consumed_offset,
            "event_time": self._now,
            "timers": self.timers,
            "checkpoint": self.checkpointed,
            "complete": self.completion,
        }

    # -- internals -----------------------------------------------------------

    def _release(
        self,
        channel: ChannelSpec,
        value: Any,
        key: str | None,
        metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None,
    ) -> None:
        # A channel with no consumer releases nothing on the edge path: on a
        # workflow that is the plugin-bound terminal, and seeding the next
        # stage from it would hand the driver a terminal output.
        if self._spec.structured_workflow:
            if channel.consumers:
                self.outputs.append(self._workflow_output(channel, channel.consumers[0], value))
        else:
            for edge in channel.consumers:
                self.outputs.extend(self._edge_outputs(channel, edge, value, key, metadata))
        if channel.sink is not None:
            self.outputs.append(self._plugin_output(channel, value, key, metadata))

    def _workflow_output(
        self, channel: ChannelSpec, edge: ConsumerEdge, value: Any
    ) -> dict[str, Any]:
        return {
            "target": "edge",
            "channel": channel.index,
            "consumer": edge.procedure,
            "plugin_channel": None,
            "key": None,
            "event_time": None,
            "metadata": [],
            "payload": _codec.encode(value),
        }

    def _edge_outputs(
        self,
        channel: ChannelSpec,
        edge: ConsumerEdge,
        value: Any,
        key: str | None,
        metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None,
    ) -> list[dict[str, Any]]:
        """One consumer's frames, one per value its transforms yield, each with
        its own key and its own clock."""
        consumer_proc = self._spec.procedures[edge.procedure].proc
        outputs = []
        # Producer-side, in declaration order: the core routes on `(edge, key)`
        # and never evaluates a host closure.
        for transformed in apply_transforms(edge.transforms, value):
            resolved = key
            if resolved is None and edge.key_by is not None:
                resolved = edge.key_by(transformed)
            if resolved is None:
                resolved = self.entity_key
            outputs.append(
                {
                    "target": "edge",
                    "channel": channel.index,
                    "consumer": edge.procedure,
                    "plugin_channel": None,
                    "key": None if resolved is None else str(resolved),
                    "event_time": (
                        consumer_proc.event_time_of(transformed)
                        if consumer_proc.event_time is not None
                        else None
                    ),
                    "metadata": self._meta_list(metadata),
                    "payload": _codec.encode(transformed),
                }
            )
        return outputs

    def _plugin_output(
        self,
        channel: ChannelSpec,
        value: Any,
        key: str | None,
        metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None,
    ) -> dict[str, Any]:
        """The plugin frame, serialized once from the value as emitted.

        An edge's transforms belong to that edge, so they never reach the sink.
        """
        resolved = key if key is not None else self.entity_key
        if channel.keyed and resolved is None:
            raise EmitError(
                f"{self._node.name}: emits on keyed channel `{channel.name}` with no key, and "
                f"has no entity key of its own to fall back on; pass `key=` on the emit"
            )
        resolved = None if resolved is None else str(resolved)
        # `sink` sets the encoder with the sink, so a plugin channel has one.
        assert channel.sink_encode is not None
        stamps = self._meta_list(metadata) + self._identity_stamps(channel, resolved)
        self._emit_ordinal += 1
        return {
            "target": "plugin",
            "channel": None,
            "consumer": None,
            "plugin_channel": channel.name,
            "key": resolved,
            "event_time": None,
            "metadata": stamps,
            "payload": channel.sink_encode(value),
        }

    def _identity_stamps(self, channel: ChannelSpec, key: str | None) -> list[tuple[str, str]]:
        """The output identity a keyed plugin frame carries, off the tier.

        Applied after user metadata, which is what makes them unspoofable: the
        reserved namespace is refused on the way in, and these are written on
        the way out.
        """
        if not channel.keyed or self._spec.effectively_once:
            return []
        return [
            (ENTITY_STAMP, f"{self._operative}/{self._node.name}/{self.entity_key or ''}"),
            (SEQ_STAMP, f"{self._change_seq}.{self._emit_ordinal}"),
            (KEY_STAMP, key or ""),
        ]

    def _meta_list(
        self, metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None
    ) -> list[tuple[str, str]]:
        if not metadata:
            return []
        pairs = metadata.items() if isinstance(metadata, Mapping) else metadata
        out: list[tuple[str, str]] = []
        for key, value in pairs:
            if str(key).startswith(RESERVED_PREFIX):
                raise EmitError(
                    f"{self._node.name}: metadata key `{key}` is inside the reserved "
                    f"`{RESERVED_PREFIX}` namespace, which the runtime stamps itself (entity "
                    f"identity, sequence, key and offset); choose a key outside it"
                )
            out.append((str(key), str(value)))
        return out
