"""The invocation context: every deterministic effect a handler is allowed.

The context accumulates one item's effects — recorded changes, outputs, timer
operations, the checkpoint flag — and the dispatcher turns them into the item
effect the core folds. Nothing here reaches the wire on its own; an emit is a
value in a list until the completion carries it.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar

from . import codec as _codec
from .builder import DROPPED, ChannelSpec, ClassSpec, ConsumerEdge, TaskNode, apply_transforms
from .errors import FlezhaError, NonRetryableError
from .identity import cell_identity, stable_id

if TYPE_CHECKING:
    from .codec import ValueCodec
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
# Contravariant, because the output appears only as what `emit` takes.
O_contra = TypeVar("O_contra", contravariant=True)

#: Metadata keys the runtime owns. A handler that sets one is refused.
RESERVED_PREFIX = "flz."

#: The identity stamps a non-tier keyed plugin emit carries.
#:
#: Under the effectively-once tier, and for a durable steps relay, the controller
#: derives them at flush time from the effect record, so none ride on the frame;
#: otherwise the frame is written straight to the wire and the stamps have
#: nowhere else to come from.
#: Byte-shape and condition both mirror the Java host, because an output
#: deduplicates on them and two SDKs disagreeing about the shape is an output that
#: silently stops deduplicating.
CELL_STAMP = "flz.cell"
SEQ_STAMP = "flz.seq"
KEY_STAMP = "flz.key"

#: The item index of an invocation that runs no work item; the core refuses its
#: part.
NO_ITEM = 2**32 - 1

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
    """The answer to one ``ctx.spawn_steps``, read inside the invocation that
    made it.

    ``wait()`` blocks for the answer; ``try_result()`` never waits and answers
    ``None`` while it is pending. Either raises :class:`~flezha.errors.FlezhaError`
    when the spawn reached no instance, its ``code`` the controller's (such as
    ``relay.already_running``) or the core's (``spawn.timeout``,
    ``spawn.unavailable``). A resolved handle answers every read the same until
    the invocation ends, which releases it: a later read raises
    ``runtime.spawn_handle_released``, and a failure no read collected is
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
        return SpawnResult(fields["id"], fields["outcome"])


class Ctx(Generic[S, O_contra]):
    """What a handler is handed alongside its input: `Ctx[State, Output]`.

    ``None`` in either position means no state or no output.
    """

    __slots__ = (
        "_spec",
        "_node",
        "_outgoing",
        "_relay",
        "_change_seq",
        "_now",
        "_bridge",
        "_token",
        "_item",
        "_offset",
        "_identity",
        "_ordinal",
        "_emit_ordinal",
        "_compensating",
        "changes",
        "outputs",
        "timers",
        "cell_key",
        "replay",
        "state",
        "checkpointed",
        "completion",
    )

    def __init__(
        self,
        *,
        spec: ClassSpec,
        node: TaskNode,
        outgoing: list[ChannelSpec],
        relay: str,
        cell_key: str | None,
        change_seq: int,
        now: int,
        replay: bool,
        state: S,
        bridge: Core | Any,
        token: int,
        item: int = NO_ITEM,
        offset: int | None = None,
        compensating: bool = False,
    ) -> None:
        self._spec = spec
        self._node = node
        self._outgoing = outgoing
        self._relay = relay
        self._change_seq = change_seq
        self._now = now
        self._bridge = bridge
        self._token = token
        # The index of this invocation's work item in its activation, which a
        # part names.
        self._item = item
        # The input-log offset of the item this invocation runs: a spawn
        # carries it so a crash re-run of the same input resolves to the worker
        # the first run spawned.
        self._offset = offset
        self._identity: bytes | None = None
        self._ordinal = 0
        # Separate from the `stable_id` ordinal on purpose: one counts
        # identifiers a handler asked for, the other counts frames it produced,
        # and a handler that does one and not the other must not shift the
        # sequence the output deduplicates on.
        self._emit_ordinal = 0
        self._compensating = compensating
        self.cell_key = cell_key
        self.replay = replay
        #: The cell's state. Mutated only through :meth:`change`: a field
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
            self._identity = cell_identity(self._relay, self._node.name, self.cell_key)
        answer = stable_id(self._identity, self._change_seq, self._ordinal)
        self._ordinal += 1
        return answer

    # -- effects ------------------------------------------------------------

    def change(self, change: Any) -> None:
        """Record one declared change: journaled, then folded into the state."""
        task = self._node.task
        if task.apply_change is None:
            raise FlezhaError("runtime.change_without_apply_change", {"tasklet": self._node.name})
        self.changes.append(task.encode_change(change))
        # Fold it live through the same path the restore fold uses, so the live
        # path and the replay path cannot drift.
        task.fold_change(self.state, change)

    def emit(
        self,
        value: O_contra,
        *,
        key: str | None = None,
        channel: str | None = None,
        metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None = None,
    ) -> None:
        """Emit one output value on the tasklet's declared output.

        ``channel`` selects one when the tasklet produces on several.
        """
        targets = [c for c in self._outgoing if c.name == channel] if channel else self._outgoing
        if not targets:
            raise (
                FlezhaError(
                    "runtime.channel_not_produced", {"tasklet": self._node.name, "channel": channel}
                )
                if channel
                else FlezhaError("runtime.no_output_channel", {"tasklet": self._node.name})
            )
        for target in targets:
            self._release(target, value, key, metadata)

    def part(self, value: O_contra) -> None:
        """Send a part: a preview of the output this invocation is producing,
        which leaves now rather than when the handler returns.

        It previews the tasklet's own output channel and is encoded as
        :meth:`emit` would encode ``value`` there. It never waits: a part that
        cannot leave at once is dropped, and a replayed input sends none. A
        listener receives it, numbered from 0 within each attempt; an output never
        does, and nothing about it is durable. Raises
        :class:`~flezha.errors.FlezhaError` coded ``part.no_output`` when the
        output channel does not end in an output (a call end is not one),
        and ``part.outside_invocation`` through a context kept past its
        invocation.
        """
        # The core previews the tasklet's first output channel and refuses one
        # that ends in no output, so a channel with no plugin end sends no bytes.
        channel = self._outgoing[0] if self._outgoing else None
        payload = (
            channel.sink_encode(value)
            if channel is not None and channel.sink_encode is not None
            else b""
        )
        self._bridge.part(self._token, self._item, payload)

    def timer_after(self, milliseconds: int) -> int:
        """Register a processing-time timer ``milliseconds`` from ``now``.

        Anchored on ``now`` and never on an ambient read: that is what makes a
        replayed invocation register the timestamp the live one did. Inside a
        ``compensate`` hook each timer call raises ``runtime.timers_unavailable``.
        """
        at = self._now + int(milliseconds)
        self._timer({"kind": "processing_time", "scheduled_at": at, "register": True})
        return at

    def timer_at_event_time(self, at: int) -> int:
        """Register an event-time timer at an absolute time."""
        at = int(at)
        self._timer({"kind": "event_time", "scheduled_at": at, "register": True})
        return at

    def cancel_timer(self, kind: str, at: int) -> None:
        """Cancel a registered timer by the value its registration answered."""
        self._timer({"kind": kind, "scheduled_at": int(at), "register": False})

    def checkpoint(self) -> None:
        """Close the open checkpoint interval (checkpoint ack-granularity classes)."""
        self.checkpointed = True

    def complete(self, result: Any = None) -> None:
        """End the instance this tasklet runs in, with ``result``.

        The rest of the iteration still runs and commits; the instance then
        ends instead of waiting for its next input, a flow instance once every
        Cell is idle. In a ``once`` steps relay it sets the result the pass
        ends with. ``result`` is encoded as an output naming no codec encodes a
        value: bytes as they are, a string as UTF-8 and anything else as JSON;
        ``None`` carries no result. A later call in the same invocation replaces
        an earlier one.
        """
        self.completion = (
            b"" if result is None else _codec.plain_payload(result, f"tasklet '{self._node.name}'")
        )

    def spawn_steps(
        self,
        factory_id: str,
        payload: Any,
        *,
        id: str | None = None,
        if_running: IfRunning | None = None,
    ) -> SpawnHandle:
        """Start a steps instance of ``factory_id`` with ``payload`` as its
        first message, and answer a :class:`SpawnHandle` at once.

        ``payload`` is encoded as an output naming no codec encodes a value, the
        rule :meth:`complete` applies: bytes as they are, a string as UTF-8 and
        anything else as JSON. It enters the instance's first entry channel and
        is decoded by that channel's codec, so a ``text`` entry reads a string
        and a ``json`` entry reads a dict.

        Without ``id`` the controller generates one and always starts a new
        instance: it can never deliver into a running one, a reissue after a
        lost or timed-out answer starts a second instance, and a replay answers
        an empty id. A caller who needs at most one instance, or the id on
        replay, passes its own ``id``. With one, ``if_running`` decides what a
        running instance under it gets: ``"deliver"`` (the default) feeds it
        ``payload``, ``"error"`` resolves the handle to a
        :class:`~flezha.errors.FlezhaError` coded ``relay.already_running``.

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
                _codec.plain_payload(payload, f"tasklet '{self._node.name}'"),
                if_running or "",
                self.replay,
                self._offset,
            ),
        )

    def effect(self, name: str, fn: Callable[[], Any], codec: ValueCodec | None = None) -> Any:
        """Run ``fn``, a call to anything outside the instance (a model, a tool,
        a service), and keep its result: when this invocation runs again (a
        crash re-run, a replay, a wake, a move), the call returns the kept result
        and ``fn`` does not run.

        The result is kept, synced on the controller, before this returns. A
        crash between ``fn`` finishing and that point runs ``fn`` again, so give
        the outside service :meth:`stable_id` as an idempotency key where it
        takes one. ``name`` names the call: a non-empty string of at most 255
        bytes, counted per invocation, so a call of another name added before
        this one leaves its kept result where it was. One call runs at a time;
        a concurrent one is refused.

        The result is encoded as an output naming no codec encodes a value
        (bytes as they are, a string as UTF-8, anything else as JSON) and read
        back as JSON, else text, else bytes; ``codec`` (a
        :class:`~flezha.codec.ValueCodec`) encodes and decodes it instead. The
        call returns the result as it reads back, on a first run and a re-run
        alike. For an ``async def`` function the call is awaitable.

        An invocation that took no input-log offset (local mode, a relay with no
        input log) runs ``fn`` each time and keeps nothing. ``fn``'s own error
        propagates and nothing is kept. Raises :class:`~flezha.errors.FlezhaError`
        coded ``effect.bad_name``, ``effect.nested``, ``effect.outside_invocation``,
        ``effect.too_large`` and ``effect.not_recorded`` (the only one retried).
        """
        if inspect.iscoroutinefunction(fn):
            return self._effect_async(name, fn, codec)
        status, kept = self._effect_begin(name)
        if status == "kept":
            return _effect_decode(kept, codec)
        try:
            value = fn()
        except BaseException:
            self._bridge.effect_release(self._token, self._item)
            raise
        return self._effect_finish(name, status, value, codec)

    async def _effect_async(
        self, name: str, fn: Callable[[], Any], codec: ValueCodec | None
    ) -> Any:
        status, kept = self._effect_begin(name)
        if status == "kept":
            return _effect_decode(kept, codec)
        try:
            value = await fn()
        except BaseException:
            self._bridge.effect_release(self._token, self._item)
            raise
        # The keep waits for the controller, so it waits off the event loop.
        return await asyncio.to_thread(self._effect_finish, name, status, value, codec)

    def _effect_begin(self, name: str) -> tuple[str, bytes | None]:
        with _effect_refusals():
            return self._bridge.effect_begin(self._token, self._item, name)

    def _effect_finish(self, name: str, status: str, value: Any, codec: ValueCodec | None) -> Any:
        """Keep a run's result where its invocation is recorded, and answer it
        as it reads back."""
        try:
            encoded = (
                codec.encode(value)
                if codec is not None
                else _codec.plain_payload(value, f"effect '{name}'")
            )
        except BaseException:
            self._bridge.effect_release(self._token, self._item)
            raise
        if status == "unrecorded":
            self._bridge.effect_release(self._token, self._item)
            return _effect_decode(encoded, codec)
        with _effect_refusals():
            first = self._bridge.effect_end(self._token, self._item, encoded)
        return _effect_decode(encoded if first is None else first, codec)

    # -- what the dispatcher collects ---------------------------------------

    def settle_return(self, returned: Any, hook: str) -> None:
        """Settle what a hook returned: the runtime's call, never the author's.

        A short form's function returns its output: ``None`` emits nothing, and
        a value with no outgoing channel is left alone, as the reference leaves
        an unconsumed output. A tasklet class hands a value on only through
        :meth:`emit`, so a value its hook returns fails the invocation without
        a retry rather than vanish. One method, because the dispatcher and the
        test harness must not disagree about it.
        """
        if returned is None:
            return
        task = self._node.task
        if not task.function_body:
            raise NonRetryableError(
                f"{task.name}: `{hook}` returned a {type(returned).__name__}; a value leaves "
                f"a tasklet only through `ctx.emit`, so `{hook}` returns None"
            )
        if returned is not DROPPED and self._outgoing:
            self.emit(returned)

    def effect_record(self, consumed_offset: int | None) -> dict[str, Any]:
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

    def _timer(self, operation: dict[str, Any]) -> None:
        # A rollback is no place to arm a deadline.
        if self._compensating:
            raise FlezhaError("runtime.timers_unavailable")
        self.timers.append(operation)

    def _release(
        self,
        channel: ChannelSpec,
        value: Any,
        key: str | None,
        metadata: Mapping[str, Any] | Iterable[tuple[str, Any]] | None,
    ) -> None:
        # In steps a value rides the edge path to its first consumer, and an
        # inter-tasklet terminal's value to the core as the pass's handoff (a
        # chained loop's carry); a plugin-bound terminal releases nothing there.
        if self._spec.runs_in_steps:
            if channel.consumers:
                consumer = channel.consumers[0].tasklet
            elif channel.kind == "inter_task" and channel.sink is None:
                # The core reads no consumer for a terminal's handoff.
                consumer = channel.producer
            else:
                consumer = None
            if consumer is not None:
                self.outputs.append(self._steps_output(channel, consumer, value))
        else:
            for edge in channel.consumers:
                self.outputs.extend(self._edge_outputs(channel, edge, value, key, metadata))
        if channel.call_end is not None:
            # A call end's requests: the value through the call chain, each
            # keyed by the chain alone. The core stamps them as calls at settle.
            end = channel.call_end
            for request in apply_transforms(end.transforms, value):
                chain_key = None if end.key_by is None else end.key_by(request)
                self.outputs.append(self._plugin_output(channel, request, chain_key, metadata))
        elif channel.sink is not None:
            resolved = key if key is not None else self.cell_key
            self.outputs.append(self._plugin_output(channel, value, resolved, metadata))

    def _steps_output(self, channel: ChannelSpec, consumer: int, value: Any) -> dict[str, Any]:
        return {
            "target": "edge",
            "channel": channel.index,
            "consumer": consumer,
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
        consumer_task = self._spec.tasklets[edge.tasklet].task
        outputs = []
        # Producer-side, in declaration order: the core routes on `(edge, key)`
        # and never evaluates a host closure.
        for transformed in apply_transforms(edge.transforms, value):
            resolved = key
            if resolved is None and edge.key_by is not None:
                resolved = edge.key_by(transformed)
            if resolved is None:
                resolved = self.cell_key
            outputs.append(
                {
                    "target": "edge",
                    "channel": channel.index,
                    "consumer": edge.tasklet,
                    "plugin_channel": None,
                    "key": None if resolved is None else str(resolved),
                    "event_time": (
                        consumer_task.event_time_of(transformed)
                        if consumer_task.event_time is not None
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

        An edge's transforms belong to that edge, so they never reach the output.
        """
        if channel.keyed and key is None:
            raise FlezhaError("runtime.key_required_on_keyed_channel", {"channel": channel.name})
        resolved = None if key is None else str(key)
        # `output` sets the encoder with the output, so a plugin channel has one.
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
        """The output identity a keyed plugin frame carries when the class
        records no effects.

        Applied after user metadata, which is what makes them unspoofable: the
        reserved namespace is refused on the way in, and these are written on
        the way out.
        """
        if not channel.keyed or self._spec.records_effects:
            return []
        return [
            (CELL_STAMP, f"{self._relay}/{self._node.name}/{self.cell_key or ''}"),
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
                raise FlezhaError("runtime.reserved_metadata_key", {"key": str(key)})
            out.append((str(key), str(value)))
        return out


@contextlib.contextmanager
def _effect_refusals() -> Iterator[None]:
    """Mark an ``effect.*`` refusal non-retryable, but for ``effect.not_recorded``:
    a call that could not be kept is the one worth running again."""
    try:
        yield
    except FlezhaError as error:
        if error.code.startswith("effect.") and error.code != "effect.not_recorded":
            error.non_retryable = True  # type: ignore[attr-defined]
        raise


def _effect_decode(body: Any, codec: ValueCodec | None) -> Any:
    """A kept result as it reads back: through ``codec``, or JSON, else text,
    else bytes."""
    if codec is not None:
        return codec.decode(body)
    raw = bytes(body)
    try:
        return json.loads(raw)
    except ValueError:
        pass
    try:
        return raw.decode()
    except UnicodeDecodeError:
        return raw
