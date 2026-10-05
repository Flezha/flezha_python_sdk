"""What a declared tasklet resolves to, and the decorator that declares one.

A tasklet is a named class whose ``run(self, value, ctx)`` is the handler and
whose other methods are the hooks, so a declaration and its implementation
cannot disagree about which ones exist. A value leaves it only through
``ctx.emit``:

.. code-block:: python

    @flezha.tasklet(name="upcase")
    class Upcase:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value.upper())

The one other body is a short form's function, whose return value is its
output; :mod:`flezha.relay` builds that one. Both produce a
:class:`Tasklet`, and everything downstream — the graph, the dispatcher, the
harness — sees only that.
"""

from __future__ import annotations

import inspect
import types
from collections.abc import Awaitable, Callable, Iterable
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Protocol,
    TypedDict,
    TypeVar,
    Union,
    Unpack,
    get_args,
    get_origin,
    is_typeddict,
    overload,
)

from . import codec as _codec
from .cell_state import runtime_mutation
from .errors import FlezhaError

if TYPE_CHECKING:
    from .ctx import Ctx

__all__ = ["Tasklet", "tasklet", "positional_parameters"]

I = TypeVar("I")
O = TypeVar("O")
# The class-spelling protocol is matched, never constructed, so its parameters
# are declared with the variance a consumer/producer position implies.
I_contra = TypeVar("I_contra", contravariant=True)
O_co = TypeVar("O_co", covariant=True)

Clock = str


class TaskletSettings(TypedDict, total=False):
    """A tasklet's own declarations beyond its name.

    One list for every surface that declares a tasklet: ``flezha.tasklet``
    and the short form of both relay decorators.
    """

    logical_clock: bool
    cell_keys: Iterable[str] | None
    idle_ttl_seconds: int | None
    max_cells: int | None
    mailbox_capacity: int | None
    schema_version: int | None
    min_readable_schema_version: int | None
    state: type | None
    change: type | None


class _ClassHandler(Protocol[I_contra, O_co]):
    """The shape a tasklet class has: the output is its context's."""

    def run(self, value: I_contra, ctx: Ctx[Any, O_co], /) -> Awaitable[None] | None: ...


class _Declare(Protocol):
    """What ``flezha.tasklet(name=...)`` answers: the typed class decorator."""

    def __call__(self, target: type[_ClassHandler[I, O]], /) -> Tasklet[I, O]: ...


def positional_parameters(fn: Callable[..., Any], *, skip_self: bool = True) -> list[str]:
    """The names of a callable's positional parameters.

    Every declaration this SDK reads is read from a signature — how many
    positional parameters a handler takes, which one carries the input, which
    one carries the context — so the walk lives here and its readers ask
    it rather than each spelling `POSITIONAL_OR_KEYWORD` again.
    """
    return [
        p.name
        for p in inspect.signature(fn).parameters.values()
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        and not (skip_self and p.name == "self")
    ]


class Tasklet(Generic[I, O]):
    """One declared tasklet: its handler, its hooks and its declarations.

    Holds no per-cell state — a cell's state lives in the dispatcher's
    registry and is handed to each invocation — so one instance per declaration
    is enough and the dispatcher allocates nothing per activation.
    """

    __slots__ = (
        "name",
        "handler",
        "function_body",
        "clock",
        "cell_keys",
        "idle_ttl_seconds",
        "max_cells",
        "mailbox_capacity",
        "schema_version",
        "min_readable_schema_version",
        "state_type",
        "change_type",
        "state_codec",
        "change_codec",
        "init_state",
        "apply_change",
        "cells_for",
        "event_time",
        "on_timer",
        "compensate",
        "migrate_state",
        "migrate_change",
        "_state_class",
        "_target",
        "__name__",
        "__wrapped__",
        "doc",
    )

    def __init__(
        self,
        target: Any,
        *,
        name: str | None = None,
        logical_clock: bool = False,
        cell_keys: Iterable[str] | None = None,
        idle_ttl_seconds: int | None = None,
        max_cells: int | None = None,
        mailbox_capacity: int | None = None,
        schema_version: int | None = None,
        min_readable_schema_version: int | None = None,
        state: type | None = None,
        change: type | None = None,
    ) -> None:
        self._target = target
        #: A short form's function, whose return value is its output. Every
        #: other tasklet is a class, whose value leaves only through `ctx`.
        self.function_body = not inspect.isclass(target)
        instance: Any = None if self.function_body else target()
        self.handler: Callable[..., Any] = _handler_of(target, instance)
        self.name = _required_name(target, name)
        self.__name__ = self.name
        #: The handler's own docstring. Not `__doc__`, which `__slots__` may
        #: not name on a class that has one of its own.
        self.doc = target.__doc__
        self.__wrapped__ = self.handler

        self.cell_keys = list(cell_keys) if cell_keys is not None else None
        self.idle_ttl_seconds = idle_ttl_seconds
        self.max_cells = max_cells
        self.mailbox_capacity = mailbox_capacity
        self.schema_version = schema_version
        self.min_readable_schema_version = min_readable_schema_version

        hook = _hooks_of(instance)
        self.init_state: Callable[[str | None], Any] | None = hook("init_state")
        self.apply_change: Callable[[Any, Any], Any] | None = hook("apply_change")
        self.cells_for: Callable[[Any], Iterable[str]] | None = hook("cells_for")
        self.event_time: Callable[[Any], int] | None = hook("event_time")
        self.on_timer: Callable[..., Any] | None = hook("on_timer")
        self.compensate: Callable[..., Any] | None = hook("compensate")
        self.migrate_state: Callable[[int, Any], Any] | None = hook("migrate_state")
        self.migrate_change: Callable[[int, Any], Any] | None = hook("migrate_change")
        self.clock = self._clock(logical_clock)
        self._declare_types(state, change)
        self._validate()

    def _declare_types(self, state: type | None, change: type | None) -> None:
        """Type hints drive the codec exactly as they drive the graph: the
        state is what the context names and the change is what `apply_change`
        folds, so a declaration cannot disagree with the code."""
        declared = _state_annotation(self, state)
        self._state_class = _buildable(declared)
        self.state_type = _declared_type(declared)
        self.change_type = change or _parameter_type(self.apply_change, 1)
        _refuse_union_change(self, change)
        self.state_codec = _codec.codec_for(self.state_type, "state")
        self.change_codec = _codec.codec_for(self.change_type, "change")

    # -- the ordinary callable -------------------------------------------

    def __call__(self, value: I, ctx: Ctx[Any, O] | None = None) -> Any:
        """Invoke the handler directly, as an ordinary Python callable.

        A short form's function takes the value alone, which is what keeps it
        unit-testable with no runtime present; a class's `run` takes a
        context, which :class:`flezha.testing.TaskletTestHarness` supplies.
        """
        if self.function_body:
            return self.handler(value)
        if ctx is None:
            raise TypeError(
                f"{self.name} takes a context; drive it with flezha.testing.TaskletTestHarness"
            )
        return self.handler(value, ctx)

    def __repr__(self) -> str:
        return f"<flezha tasklet {self.name}>"

    # -- what the dispatcher and the graph ask ---------------------------

    @property
    def stateful(self) -> bool:
        """Whether each cell holds a state: `init_state` written, or a state class."""
        return self.init_state is not None or self._state_class is not None

    def build_initial_state(self, cell_key: str | None) -> Any:
        """A new cell's state: `init_state(cell_key)`, else the state class
        called with no arguments, else none."""
        if self.init_state is not None:
            return self.init_state(cell_key)
        return self._state_class() if self._state_class is not None else None

    def fold_change(self, state_value: Any, change: Any) -> None:
        """Fold one change into the live state, through the runtime's own path.

        ``apply_change`` mutates in place and its answer is ignored — the same
        contract Rust's ``&mut S`` and the other SDKs carry — so a scalar state
        is declared as a class or a dict rather than reassigned here.
        """
        assert self.apply_change is not None
        with runtime_mutation():
            self.apply_change(state_value, change)

    def encode_state(self, value: Any) -> bytes:
        """One durable state body, through the declared codec or the default.

        The four encode/decode pairs live here because this is where the codecs
        are resolved; the dispatcher, the restore fold, the context and the test
        harness all ask rather than each repeating the choice.
        """
        return self.state_codec.encode(value) if self.state_codec else _codec.encode(value)

    def decode_state(self, body: Any) -> Any:
        return self.state_codec.decode(body) if self.state_codec else _codec.decode(body)

    def encode_change(self, value: Any) -> bytes:
        return self.change_codec.encode(value) if self.change_codec else _codec.encode(value)

    def decode_change(self, body: Any) -> Any:
        return self.change_codec.decode(body) if self.change_codec else _codec.decode(body)

    def event_time_of(self, value: Any) -> int:
        assert self.event_time is not None
        return int(self.event_time(value))

    def cells_of(self, value: Any) -> list[str]:
        assert self.cells_for is not None
        answer = self.cells_for(value)
        if isinstance(answer, (str, bytes)):
            return [answer if isinstance(answer, str) else answer.decode()]
        return [str(key) for key in answer]

    # -- registration-time refusals owned by the tasklet ---------------

    def _refuse(self, defect: str) -> None:
        raise _invalid(self.name, defect)

    def _clock(self, logical_clock: bool) -> Clock:
        """The clock the class writes: `event_time`, `logical_clock=True`, or the wall."""
        if logical_clock and self.event_time is not None:
            raise FlezhaError("topology.two_clocks", {"tasklet": self.name})
        if self.event_time is not None:
            return "event_time"
        return "logical" if logical_clock else "wall"

    def _validate(self) -> None:
        if self.cell_keys is not None and not self.cell_keys:
            raise FlezhaError("topology.static_tasklet_without_cell_keys", {"tasklet": self.name})
        self._validate_idle_ttl()
        # State without a fold is legitimate: a value seeded at start and read
        # but never mutated has nothing to journal, and `ctx.change` already
        # refuses at the point of use. A fold with no state to fold into is not.
        if not self.stateful and self.apply_change is not None:
            raise FlezhaError("topology.change_without_state", {"tasklet": self.name})
        _refuse_unannotated(self)
        if self.function_body:
            _refuse_context_parameter(self)
        else:
            _refuse_context_hooks(self)
        self._validate_schema()
        if not self.stateful:
            return
        # A state that cannot be built or encoded cannot be restored, and the
        # first anyone would learn of it is a restart. Build it now.
        try:
            initial = self.build_initial_state(None)
        except TypeError as error:
            if self.init_state is not None or self._state_class is None:
                raise
            raise FlezhaError(
                "topology.state_not_constructible",
                {"tasklet": self.name, "state": self._state_class.__name__},
            ) from error
        if self.state_codec is None:
            _codec.assert_representable(f"{self.name}.state", initial)

    def _validate_idle_ttl(self) -> None:
        ttl = self.idle_ttl_seconds
        if ttl is None or (isinstance(ttl, int) and not isinstance(ttl, bool) and ttl > 0):
            return
        raise FlezhaError(
            "topology.invalid_setting",
            {
                "declaration": f"tasklet '{self.name}'",
                "setting": "idle_ttl_seconds",
                "value": repr(ttl),
                "expected": "a positive whole number of seconds",
            },
        )

    def _validate_schema(self) -> None:
        """The readable floor against the running version and its migration."""
        floor = self.min_readable_schema_version
        if floor is None:
            return
        running = self.schema_version or 1
        if floor > running:
            raise FlezhaError(
                "topology.schema_floor_above_version",
                {"tasklet": self.name, "floor": floor, "version": running},
            )
        if floor < running and self.migrate_state is None and self.migrate_change is None:
            raise FlezhaError(
                "topology.schema_floor_without_migration", {"tasklet": self.name, "floor": floor}
            )


def _invalid(tasklet: str, reason: str) -> FlezhaError:
    return FlezhaError("topology.invalid_tasklet", {"tasklet": tasklet, "reason": reason})


def _handler_of(target: Any, instance: Any) -> Callable[..., Any]:
    """A function body is its own handler; a class's is its `run` method."""
    handler = target if instance is None else getattr(instance, "run", None)
    if not callable(handler):
        raise FlezhaError(
            "topology.missing_body",
            {"declaration": f"tasklet '{getattr(target, '__name__', target)}'", "body": "`run`"},
        )
    return handler


def _required_name(target: Any, name: str | None) -> str:
    if not name:
        # Written rather than derived from the class name, so a renamed
        # class keeps its state.
        raise FlezhaError(
            "topology.tasklet_name_missing", {"tasklet": getattr(target, "__name__", str(target))}
        )
    return name


def _hooks_of(instance: Any) -> Callable[[str], Any]:
    def hook(name: str) -> Any:
        return getattr(instance, name, None) if instance is not None else None

    return hook


def _refuse_context_parameter(task: Tasklet[Any, Any]) -> None:
    """A short form's function takes the input alone and returns its output,
    so a second parameter, such as a context, would be a second way out."""
    names = positional_parameters(task.handler, skip_self=False)
    if len(names) > 1:
        task._refuse(
            f"takes a second parameter `{names[1]}`; a function short form takes only its "
            f"input and returns its output. For a context, write a class whose "
            f"`run(self, value, ctx)` emits through it"
        )


def _refuse_context_hooks(task: Tasklet[Any, Any]) -> None:
    """Each context hook takes the context last and hands nothing back, and
    the retired `initial_state` hook is refused rather than silently ignored.

    A value leaves a tasklet only through ``ctx.emit``, so a hook that cannot
    reach a context, or says it returns a value, is refused before anything
    runs. A value returned anyway is refused at run time by
    :meth:`flezha.ctx.Ctx.settle_return`.
    """
    if hasattr(task._target, "initial_state"):
        raise FlezhaError(
            "topology.retired_callback",
            {
                "tasklet": task.name,
                "callback": "`initial_state`",
                "instead": "`init_state(self, cell_key)`",
            },
        )
    hooks = {"run": task.handler, "on_timer": task.on_timer, "compensate": task.compensate}
    for hook, fn in hooks.items():
        if fn is None:
            continue
        names = positional_parameters(fn)
        if len(names) < 2:
            first = names[0] if names else "value"
            task._refuse(
                f"`{hook}` takes no context parameter; write `{hook}(self, {first}, ctx)`, "
                f"since a value leaves a tasklet only through `ctx.emit`"
            )
        returned = _codec.type_hints(fn).get("return", None)
        if returned is not None and returned is not type(None) and returned != "None":
            task._refuse(
                f"`{hook}` is annotated to return `{returned}`; a value leaves a tasklet only "
                f"through `ctx.emit`, so `{hook}` returns None"
            )


def _state_annotation(task: Tasklet[Any, Any], state: type | None) -> Any:
    """The state a tasklet declares: `state=`, else the state argument of
    run's context hint, else what `init_state` returns."""
    if state is not None:
        return state
    declared = _context_argument(task, 0)
    if _names_a_type(declared):
        return declared
    if task.init_state is None:
        return None
    return _codec.type_hints(task.init_state).get("return")


def _context_argument(task: Tasklet[Any, Any], index: int) -> Any:
    """One argument of a class's context hint, `Ctx[State, Output]`: 0 for the
    state, 1 for the output; ``None`` when the hint has no such argument."""
    if task.function_body:
        return None
    arguments = get_args(_parameter_annotation(task.handler, 1))
    return arguments[index] if len(arguments) == 2 else None


def _names_a_type(annotation: Any) -> bool:
    return annotation not in (None, type(None), Any, object) and not isinstance(annotation, str)


def _buildable(annotation: Any) -> type | None:
    """The class a new cell's state is built by, when it is called with no
    arguments: the declared class, or a parameterised builtin's origin."""
    if not _names_a_type(annotation):
        return None
    if isinstance(annotation, type):
        return annotation
    origin = get_origin(annotation)
    if isinstance(origin, type) and origin is not types.UnionType:
        return origin
    return None


def _declared_type(annotation: Any) -> type | None:
    """The class an annotation names, or ``None`` for "undeclared".

    ``Any`` and ``object`` are *not* declarations: they say the author did not
    name a type, and the answer to that is the MessagePack default rather than
    a refusal. (`Any` is a class from 3.11 on, so the plain `isinstance` check
    accepts it and the codec then refuses a type nobody declared.) A union is
    not a declaration either, and is refused where it is used, because a decoded
    body has nothing in it that says which arm wrote it. A ``TypedDict`` is a
    plain dict at run time, so it takes the default too.
    """
    if not _names_a_type(annotation) or is_typeddict(annotation):
        return None
    return annotation if isinstance(annotation, type) else None


def _parameter_annotation(fn: Callable[..., Any] | None, index: int) -> Any:
    if fn is None:
        return None
    names = positional_parameters(fn)
    if index >= len(names):
        return None
    return _codec.type_hints(fn).get(names[index])


def _parameter_type(fn: Callable[..., Any] | None, index: int) -> type | None:
    return _declared_type(_parameter_annotation(fn, index))


def input_type(task: Tasklet[Any, Any]) -> Any:
    """The type the handler accepts, as its annotation names it."""
    return _codec.type_hints(task.handler).get(_first_parameter(task.handler))


def output_type(task: Tasklet[Any, Any]) -> Any:
    """The type a tasklet hands on: a function's return annotation, or the
    output argument of a class's context hint, `Ctx[State, Output]`.

    An unresolvable hint degrades to the raw annotation, as every hint does.
    """
    if task.function_body:
        return _codec.type_hints(task.handler).get("return")
    context = _parameter_annotation(task.handler, 1)
    # A bare or absent `Ctx` names no output; only an unresolvable hint keeps its text.
    return context if isinstance(context, str) else _context_argument(task, 1)


def _first_parameter(fn: Callable[..., Any]) -> str:
    names = positional_parameters(fn)
    return names[0] if names else ""


def _refuse_union_change(task: Tasklet[Any, Any], declared: type | None) -> None:
    """A union change type is refused, naming the two ways to spell a sum.

    A durable body carries the codec's bytes and nothing that says which arm of
    a union wrote them, so a union would decode to whichever arm happened to be
    tried first — a defect that surfaces only on a restore. Every SDK answers
    this the same way: one Change type per tasklet. Rust's is an enum, and
    Python's is either one class with the variants as fields, or a plain dict.
    """
    if declared is not None:
        return
    annotation = _parameter_annotation(task.apply_change, 1)
    if annotation is None or get_origin(annotation) not in (Union, types.UnionType):
        return
    raise _invalid(
        task.name,
        f"declares its change as the union `{annotation}`; a durable body carries no tag "
        f"saying which arm wrote it, so the restore would guess. Declare one change class "
        f"whose fields cover the variants, or annotate it `Any` and record plain dicts",
    )


def _refuse_unannotated(task: Tasklet[Any, Any]) -> None:
    """A handler parameter with no annotation cannot be wired.

    The graph is built from the hints, so an unannotated parameter is not a
    style question — it is a wiring the SDK cannot check and a type checker
    cannot see. Refused here, naming the handler and the parameter, rather than
    silently wiring anything to anything.
    """
    name = _first_parameter(task.handler)
    if not name:
        raise _invalid(
            task.name,
            "takes no input parameter; a tasklet is what runs against an input",
        )
    if name not in _codec.type_hints(task.handler):
        raise _invalid(
            task.name,
            f"parameter `{name}` has no type annotation; the graph is wired from the hints, "
            f"so an unannotated input cannot be checked against the edge that feeds it",
        )


# ---------------------------------------------------------------------------
# The decorator
# ---------------------------------------------------------------------------

#: The keywords of a relay's short form, which a tasklet never takes.
_RELAY_KEYWORDS = ("run", "durable", "output", "input", "codec")


@overload
def tasklet(
    target: type[_ClassHandler[I, O]], /, *, name: str, **settings: Unpack[TaskletSettings]
) -> Tasklet[I, O]: ...


@overload
def tasklet(*, name: str, **settings: Unpack[TaskletSettings]) -> _Declare: ...


def tasklet(target: Any = None, /, **declarations: Any) -> Any:
    """Declare a tasklet: a named class whose ``run(self, value, ctx)`` is the
    handler and whose other methods are the hooks.

    Declaring performs no I/O, opens no socket and starts no thread: it builds
    one :class:`Tasklet` and returns it. A function is refused: a whole
    one-tasklet relay written as a function is the short form of
    :func:`~flezha.relay.relay`.
    """
    written = [key for key in _RELAY_KEYWORDS if key in declarations]
    if written:
        raise FlezhaError(
            "topology.relay_field_on_tasklet",
            {
                "tasklet": declarations.get("name") or getattr(target, "__name__", "<tasklet>"),
                "fields": [f"`{key}`" for key in written],
                "instead": "`@flezha.relay(id=...)`",
            },
        )

    def declare(inner: Any) -> Tasklet[Any, Any]:
        if not inspect.isclass(inner):
            raise _invalid(
                getattr(inner, "__name__", str(inner)),
                "is a function; a tasklet is a class whose `run(self, value, ctx)` takes the "
                "value and the context. A whole one-tasklet relay written as a function "
                "is `@flezha.relay(id=...)`",
            )
        return Tasklet(inner, **declarations)

    return declare if target is None else declare(target)
