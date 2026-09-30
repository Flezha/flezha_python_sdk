"""Turning one activation into one answer, exactly once.

Per-cell exclusivity IS the handout: the core holds a cell's next
activation until this one is answered, so an activation that returns unanswered
retires that cell for the life of the process. Every path out of
:func:`dispatch` therefore ends in a completion or an abandon, through a
``finally`` rather than a rule applied at each return site — a raised handler,
an encoding failure and a killed thread all reach the same guard.
"""

from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from . import codec as _codec
from . import determinism
from .builder import ChannelSpec, ClassSpec, ConsumerEdge, TaskNode, apply_transforms
from .cell_state import fold_restore
from .ctx import NO_ITEM, Ctx
from .errors import FlezhaError
from .logger import log
from .tasklet import Tasklet

__all__ = [
    "Registry",
    "RegisteredClass",
    "Cell",
    "DispatchDeps",
    "answer",
    "dispatch",
    "dispatch_now",
    "invoke",
    "run_now",
    "resolve",
]


@dataclass(slots=True)
class Cell:
    """One Cell of one declared class: what every activation kind operates on.

    The `(registered, node, operator, key)` address every activation kind
    names, and where `state` is read and written — so the lock stays inside
    :class:`RegisteredClass` and nothing else touches the cell table.
    """

    registered: RegisteredClass
    node: TaskNode
    operator: str
    key: str | None
    #: Where the cell's state is kept in its class's table.
    address: tuple[str, int, str | None] = field(init=False)

    def __post_init__(self) -> None:
        self.address = (self.operator, self.node.index, self.key)

    @property
    def task(self) -> Tasklet[Any, Any]:
        return self.node.task

    @property
    def name(self) -> str:
        return self.node.name

    @property
    def state(self) -> Any:
        """The live state, created from the declaration on first touch."""
        return self.registered.state_of(self)

    @state.setter
    def state(self, value: Any) -> None:
        self.registered.set_state(self, value)


class RegisteredClass:
    """A declared class, with the lookups dispatch needs, and its live cells."""

    __slots__ = ("spec", "outgoing", "_cells", "_lock")

    def __init__(self, spec: ClassSpec) -> None:
        self.spec = spec
        self.outgoing: list[list[ChannelSpec]] = [
            [channel for channel in spec.channels if channel.producer == node.index]
            for node in spec.tasklets
        ]
        # Keyed by operator too: one class serves every operator of its
        # factory on this runner, such as each bounded instance, and each has
        # cells of its own under the same tasklet and key.
        self._cells: dict[tuple[str, int, str | None], Any] = {}
        # Cells of one class are touched from every worker thread, and the
        # core's per-cell exclusivity does not extend to this dict: two
        # different cells of the same class run concurrently by design.
        self._lock = threading.Lock()

    def state_of(self, cell: Cell) -> Any:
        """The cell's live state, created from the declaration on first touch."""
        with self._lock:
            if cell.address not in self._cells:
                self._cells[cell.address] = cell.task.build_initial_state(cell.key)
            return self._cells[cell.address]

    def set_state(self, cell: Cell, state: Any) -> None:
        with self._lock:
            self._cells[cell.address] = state

    def live_instances(self) -> int:
        """How many operators of this class have a materialised cell here."""
        with self._lock:
            return len({operator for operator, _node, _key in self._cells})

    def cell(self, node: TaskNode, operator: str, key: str | None) -> Cell:
        """The handle every activation kind operates through."""
        return Cell(self, node, operator, key)

    def channel_at(self, index: int) -> ChannelSpec | None:
        """The channel an activation names, or ``None`` when it names none.

        An out-of-range index is a frame this runner's declaration does not
        describe; the caller decodes with the default rather than failing, so a
        declaration drift degrades to plain MessagePack instead of a crash.
        """
        channels = self.spec.channels
        return channels[index] if 0 <= index < len(channels) else None


class Registry:
    """The declared classes this runner serves."""

    __slots__ = ("_classes", "_lock")

    def __init__(self) -> None:
        self._classes: dict[str, RegisteredClass] = {}
        self._lock = threading.Lock()

    def add(self, spec: ClassSpec) -> None:
        with self._lock:
            if spec.factory_id in self._classes:
                raise FlezhaError("topology.duplicate_factory_id", {"factory_id": spec.factory_id})
            self._classes[spec.factory_id] = RegisteredClass(spec)

    def remove(self, factory_id: str) -> RegisteredClass:
        with self._lock:
            if factory_id not in self._classes:
                raise FlezhaError("runtime.class_not_registered", {"factory": factory_id})
            return self._classes.pop(factory_id)

    def get(self, factory_id: str) -> RegisteredClass | None:
        return self._classes.get(factory_id)

    def ids(self) -> list[str]:
        with self._lock:
            return list(self._classes)

    def all(self) -> list[RegisteredClass]:
        with self._lock:
            return list(self._classes.values())

    def __len__(self) -> int:
        return len(self._classes)


@dataclass(slots=True)
class DispatchDeps:
    """What a dispatch loop needs to answer an activation, over one extension."""

    registry: Registry | None
    bridge: Any


def failed(token: int, message: str, *, non_retryable: bool = False) -> dict[str, Any]:
    return {
        "token": token,
        "outcome": "failed",
        "error": message,
        "non_retryable": non_retryable,
    }


class UnreadableMessage(Exception):
    """An entry channel's decoder, or the entry edge's transforms over what it
    decoded, rejected a message. Only the entrance raises this: the verdict it
    becomes tells the core the message is unreadable rather than that the
    handler failed, so a bounded parks it and keeps waiting instead of ending."""

    def __init__(self, cause: BaseException) -> None:
        super().__init__(error_message(cause))


def error_message(error: BaseException) -> str:
    """What a failure says on the wire.

    ``class: message``, in family with what the other SDKs send — this string is
    stored in a dead-letter entry and read by a user, and a multi-line
    traceback there is noise the retry ladder multiplies. The traceback is
    logged host-side instead, where it is a diagnostic rather than a record.
    """
    return f"{type(error).__name__}: {error}"


def is_non_retryable(error: BaseException) -> bool:
    """A handler declares an error non-retryable by carrying ``non_retryable``."""
    return bool(getattr(error, "non_retryable", False))


_thread_state = threading.local()


def _thread_loop() -> asyncio.AbstractEventLoop:
    """This thread's own event loop, created on first need and reused.

    Only a thread that actually runs an async handler ever makes one, and it
    makes exactly one for its lifetime — a loop per activation would cost more
    than the handler on a fast one.
    """
    loop = getattr(_thread_state, "loop", None)
    if loop is None:
        loop = asyncio.new_event_loop()
        _thread_state.loop = loop
    return loop


def close_thread_loop() -> None:
    """Release this thread's event loop, if it made one. Called as a worker ends."""
    loop = getattr(_thread_state, "loop", None)
    if loop is not None:
        loop.close()
        _thread_state.loop = None


async def resolve(value: Any) -> Any:
    """Settle what a handler returned, whichever style it was written in.

    Sync and async handlers are both first class and the user never declares
    which they wrote, so this is the one place the difference is resolved — and
    it resolves it by asking where it is running rather than by being told:

    * on an event loop (the asyncio executor), the awaitable is awaited, so the
      loop keeps serving everything else while the handler waits;
    * off one (the thread executor), it is driven to completion on this
      thread's own loop, which is a blocking call on a thread whose whole job
      is this activation.

    The second branch is what lets the thread executor drive the entire
    dispatch without an event loop when nothing is async at all: it never
    suspends, so :func:`run_now` finishes it in one step.
    """
    if not inspect.isawaitable(value):
        return value
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _thread_loop().run_until_complete(value)
    return await value


def run_now(coroutine: Any) -> Any:
    """Drive a coroutine that has nothing to suspend on, with no event loop.

    The dispatch path's only await point is :func:`resolve`, which off a loop
    blocks rather than suspends — so a dispatch driven this way completes in one
    step, and the thread executor pays no event loop for a synchronous handler.
    """
    try:
        coroutine.send(None)
    except StopIteration as stop:
        return stop.value
    coroutine.close()
    raise RuntimeError(
        "the dispatch suspended off an event loop; this is a defect in the SDK's coroutine "
        "seam rather than in the handler"
    )


def values_of(channel: ChannelSpec | None, edge: ConsumerEdge | None, payload: Any) -> list[Any]:
    """A payload as the handler sees it: every value the edge yields for it.

    The entry channel's decoder, or the class codec for a payload between
    tasklets, then the consuming edge's transforms in declaration order.
    Empty when a filter rejected it. One function for the two places a payload
    crosses in — an input item, and a payload at the prepare crossing — so a
    filter cannot mean one thing on one path and another on the other.
    """
    if channel is not None and channel.decode is not None:
        decoded = channel.decode(payload)
    else:
        decoded = _codec.decode(payload)
    return apply_transforms(edge.transforms if edge else (), decoded)


async def invoke(
    cell: Cell,
    activation: dict[str, Any],
    deps: DispatchDeps,
    now: int,
    replay: bool,
    body: Callable[[Ctx[Any, Any]], Any],
    offset: int | None = None,
    item: int = NO_ITEM,
) -> tuple[Ctx[Any, Any], Any]:
    """Run user code against one cell, and keep the state it produced.

    Every activation kind that reaches a handler goes through here — an input, a
    timer firing, a compensation — so the context they receive and the state
    they leave behind cannot differ between them. The state is written back
    whether or not the body raised: a change recorded before the raise is
    already journaled, and dropping it here would make the live cell disagree
    with its own log.
    """
    ctx: Ctx[Any, Any] = Ctx(
        spec=cell.registered.spec,
        node=cell.node,
        outgoing=cell.registered.outgoing[cell.node.index],
        operator=cell.operator,
        cell_key=cell.key,
        change_seq=activation.get("change_seq", 0),
        now=now,
        replay=replay,
        state=cell.state,
        bridge=deps.bridge,
        token=activation["token"],
        item=item,
        offset=offset,
        compensating=activation.get("kind") == "compensate_run",
    )
    try:
        with determinism.invocation():
            return ctx, await resolve(body(ctx))
    finally:
        cell.state = ctx.state


async def answer(activation: dict[str, Any], deps: DispatchDeps) -> dict[str, Any]:
    """Produce the answer for one activation.

    Raising is not an option here: every failure becomes a result the core can
    act on, because the alternative is a cell that never receives work again.
    """
    token = activation["token"]
    try:
        assert deps.registry is not None
        registered = deps.registry.get(activation["factory"])
        if registered is None:
            return failed(
                token,
                f"no class registered for factory `{activation['factory']}`",
                non_retryable=True,
            )

        tasklets = registered.spec.tasklets
        index = activation["tasklet"]
        if index >= len(tasklets):
            return failed(
                token,
                f"class `{activation['factory']}` has no tasklet at index {index}",
                non_retryable=True,
            )
        cell = registered.cell(
            tasklets[index], activation.get("operator", ""), activation.get("cell_key")
        )
        return await _answer_kind(cell, activation, registered, deps)
    except Exception as error:  # noqa: BLE001 - every failure becomes a result
        return failed(token, error_message(error), non_retryable=is_non_retryable(error))


async def _answer_kind(
    cell: Cell, activation: dict[str, Any], registered: RegisteredClass, deps: DispatchDeps
) -> dict[str, Any]:
    """One arm per activation kind the boundary defines."""
    token = activation["token"]
    kind = activation["kind"]

    if kind == "work":
        return await _run_work(cell, activation, deps)
    if kind == "restore_fold":
        return _restore_fold(cell, activation)
    if kind == "snapshot_request":
        return {
            "token": token,
            "outcome": "snapshot",
            "state": cell.task.encode_state(cell.state),
        }
    # Two names for one read: a live query and a migration handover both want
    # the state as JSON, and only the outcome key differs.
    if kind in ("live_state_read", "migration_serialize"):
        outcome = "state_json" if kind == "live_state_read" else "migration_state"
        return {"token": token, "outcome": outcome, "json": _codec.to_state_json(cell.state)}
    if kind == "apply_state":
        cell.state = _codec.from_state_json(activation.get("state_json"))
        return {"token": token, "outcome": "state_applied"}
    if kind == "key_request":
        return _prepare(activation, registered)
    if kind == "compensate_run":
        return await _compensate(cell, activation, deps)
    return failed(token, f"unknown activation kind `{kind}`", non_retryable=True)


async def _run_work(cell: Cell, activation: dict[str, Any], deps: DispatchDeps) -> dict[str, Any]:
    effects: list[dict[str, Any]] = []

    for index, item in enumerate(activation.get("items") or ()):
        now = item.get("now") or 0
        try:
            ctx, _ = await invoke(
                cell,
                activation,
                deps,
                now,
                bool(item.get("replay")),
                lambda c, item=item: _run_item(cell, item, c),
                item.get("consumed_offset"),
                index,
            )
            effects.append(ctx.effect(item.get("consumed_offset")))
        except Exception as error:  # noqa: BLE001 - the verdict is the report
            # The traceback is a host diagnostic, not a record: the verdict
            # below travels to the dead-letter entry and the controller's log.
            log().debug("%s failed an item", cell.name, exc_info=error)
            # Items after this one are re-handed by the core, so the batch stops
            # here rather than reporting effects the cell never saw.
            verdict, message = _failure_of(error)
            effects.append(
                {
                    "verdict": verdict,
                    "message": message,
                    "event_time": now,
                    "consumed_offset": item.get("consumed_offset"),
                }
            )
            break

    return {"token": activation["token"], "outcome": "work", "effects": effects}


def _failure_of(error: BaseException) -> tuple[str, str]:
    """An item's failure as its verdict and the message the wire carries."""
    if isinstance(error, UnreadableMessage):
        return "unreadable", str(error)
    return ("non_retryable" if is_non_retryable(error) else "retryable"), error_message(error)


async def _run_item(cell: Cell, item: dict[str, Any], ctx: Ctx[Any, Any]) -> None:
    task = cell.task
    if item["kind"] == "timer":
        if task.on_timer is None:
            # Registration is a runtime call, so this cannot be refused at
            # declaration time.
            raise FlezhaError("runtime.timer_without_on_timer", {"tasklet": task.name})
        timer = {
            "kind": item.get("timer_kind"),
            "scheduled_at": item.get("scheduled_at") or item.get("now") or 0,
        }
        ctx.settle_return(await resolve(task.on_timer(timer, ctx)), "on_timer")
        return

    channel = cell.registered.channel_at(item.get("channel") or 0)
    # This node's own edge off the channel: a transform chain declared before
    # one `into` belongs to that consumer and to no other. It is evaluated here
    # only for a payload that reached us untransformed — an entry payload,
    # whose producer is the wire, or a bounded stage's released value. A
    # stream producer evaluated it as its activation completed, and evaluating
    # it again would compose the chain with itself.
    untransformed = (
        channel is None or channel.kind == "entry" or cell.registered.spec.structured_bounded
    )
    edge = _edge_into(channel, cell.node.index) if untransformed else None
    # The entrance (the entry channel's decoder and the entry edge's
    # transforms) is where a message the operator cannot read is told apart
    # from a handler that fails on one it can.
    try:
        values = values_of(channel, edge, item.get("payload") or b"")
    except Exception as error:
        if channel is None or channel.kind != "entry":
            raise
        raise UnreadableMessage(error) from error
    # A filtered value is consumed and reaches no handler, and a fanned one
    # reaches it once per value. Either way the item produces one effect, so
    # its offset advances once and nothing is retried.
    for value in values:
        if edge is None or _routes_to(cell, edge, value):
            ctx.settle_return(await resolve(task(value, ctx)), "run")


def _restore_fold(cell: Cell, activation: dict[str, Any]) -> dict[str, Any]:
    cell.state = fold_restore(
        cell.task,
        {"snapshot": activation.get("snapshot"), "changes": activation.get("changes")},
        cell.key,
        cell.name,
    )
    return {"token": activation["token"], "outcome": "restored"}


def _prepare(activation: dict[str, Any], registered: RegisteredClass) -> dict[str, Any]:
    request = activation.get("key_request")
    if request is None:
        return failed(activation["token"], "a key request carried no payloads", non_retryable=True)

    channel = registered.channel_at(request["channel"])
    # The crossing answers one key set for the whole payload, which the core
    # hands every keyed consumer, so it reads the first keyed consumer's edge:
    # an unkeyed one's filter must not drop the key.
    edges = channel.consumers if channel else []
    tasklets = registered.spec.tasklets
    edge = next(
        (
            e
            for e in edges
            if e.key_by is not None or tasklets[e.tasklet].task.cells_for is not None
        ),
        edges[0] if edges else None,
    )
    task = tasklets[edge.tasklet].task if edge is not None else None

    answers: list[dict[str, Any]] = []
    for payload in request["payloads"]:
        try:
            values = values_of(channel, edge, payload)
            # A filtered payload answers no key. For a dynamic consumer that is
            # what tells the core not to route it at all — the drop happens
            # before a cell is minted rather than inside one.
            if not values:
                answers.append({"keys": []})
                continue
            # A fanned payload answers every key its values name, once each.
            keys: dict[str, None] = {}
            if request["want_keys"]:
                for value in values:
                    keys.update(dict.fromkeys(_keys_of(edge, task, value) or ()))
            event_time = (
                task.event_time_of(values[0])
                if request["want_event_time"] and task is not None and task.event_time is not None
                else None
            )
            answers.append({"keys": list(keys), "event_time": event_time})
        except Exception as error:  # noqa: BLE001 - a per-payload refusal
            # A per-payload refusal enters the input channel's retry ladder; it
            # is not a failure of the crossing, whose other payloads still have
            # answers.
            answers.append(
                {"error": error_message(error), "non_retryable": is_non_retryable(error)}
            )

    return {"token": activation["token"], "outcome": "prepared", "prepared": answers}


def _edge_into(channel: ChannelSpec | None, node: int) -> ConsumerEdge | None:
    """The channel's edge that feeds `node`, or ``None`` when it feeds none."""
    if channel is None:
        return None
    return next((edge for edge in channel.consumers if edge.tasklet == node), None)


def _keys_of(
    edge: ConsumerEdge | None, task: Tasklet[Any, Any] | None, value: Any
) -> list[str] | None:
    """The cell keys one value routes to: the edge's key, else the tasklet's
    router, else ``None`` when neither is declared."""
    if edge is not None and edge.key_by is not None:
        return [str(edge.key_by(value))]
    if task is not None and task.cells_for is not None:
        return task.cells_of(value)
    return None


def _routes_to(cell: Cell, edge: ConsumerEdge, value: Any) -> bool:
    """Whether a value the edge yielded belongs to this cell.

    The core hands an untransformed payload to every cell its crossing
    named, and each re-applies the chain, so a fan across keys keeps only the
    values keyed to the cell that received it. With no cell key, or no key
    and no router to name one, every value belongs.
    """
    if cell.key is None:
        return True
    keys = _keys_of(edge, cell.task, value)
    return keys is None or cell.key in keys


async def _compensate(cell: Cell, activation: dict[str, Any], deps: DispatchDeps) -> dict[str, Any]:
    task = cell.task
    if task.compensate is None:
        return failed(
            activation["token"], f"{cell.name} declares no compensate hook", non_retryable=True
        )

    outputs = [_codec.decode(payload) for payload in activation.get("outputs") or ()]
    ctx, returned = await invoke(
        cell,
        activation,
        deps,
        0,
        False,
        lambda c: task.compensate(outputs, c),  # type: ignore[misc]
    )
    ctx.settle_return(returned, "compensate")
    return {"token": activation["token"], "outcome": "work", "effects": [ctx.effect(None)]}


def _release_views(activation: dict[str, Any]) -> None:
    """End the validity of every payload view this activation handed out.

    The packaging contract says the borrowed view's lifetime ends with the
    invocation; this is where it ends. A view a handler stashed in state then
    names the rule on its next read instead of serving bytes whose owner is
    gone.
    """
    for value in _views_in(activation):
        release = getattr(value, "release", None)
        if release is not None:
            release()


def _views_in(value: Any) -> Iterable[Any]:
    if isinstance(value, dict):
        for item in value.values():
            yield from _views_in(item)
    elif isinstance(value, list):
        for item in value:
            yield from _views_in(item)
    elif hasattr(value, "release") and hasattr(value, "tobytes"):
        yield value


async def dispatch(activation: dict[str, Any], core: Any, deps: DispatchDeps) -> None:
    """Dispatch one activation and answer it exactly once.

    The guard is the contract, not politeness. An unanswered activation retires
    its cell for the life of the process, so a raise from the dispatcher, from
    encoding the result, or from ``complete`` itself abandons rather than
    logging and returning.
    """
    answered = False
    try:
        core.complete(await answer(activation, deps))
        answered = True
    except Exception as error:  # noqa: BLE001 - the abandon below is the answer
        log().error("activation processing failed: %s", error_message(error), exc_info=error)
    finally:
        if not answered:
            try:
                core.abandon(activation["token"])
            except Exception as error:  # noqa: BLE001 - nothing is wedged
                # The core refuses an unknown token: `complete` had already
                # landed and only the bookkeeping after it raised.
                log().warning(
                    "abandoning activation %s failed: %s",
                    activation["token"],
                    error_message(error),
                )
        _release_views(activation)


def dispatch_now(activation: dict[str, Any], core: Any, deps: DispatchDeps) -> None:
    """Dispatch on this thread, with no event loop unless a handler needs one."""
    run_now(dispatch(activation, core, deps))
