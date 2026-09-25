"""The operator decorators: what an author declares and what a runner registers.

An operator is its channel graph, so the decorated object is the function that
builds one:

.. code-block:: python

    @flezha.unbounded_operator("echo")
    def echo(c: flezha.Channels) -> None:
        c.input("input").into(echo_task).sink("out")

A bounded operator is the same shape: a chain of tasklets over a graph. A
channels body takes one parameter, annotated :class:`~flezha.builder.Channels` or
not at all.

Handed a tasklet body instead, a class with ``run`` or a function of any
other shape, the same decorators declare the single-tasklet short form:

.. code-block:: python

    @flezha.unbounded_operator(id="echo", sink="out", codec="text")
    def echo(value: str) -> str:
        return value
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Literal, Unpack, get_args, get_origin

from . import codec as _codec
from .builder import Channels, ClassSpec
from .errors import DeclarationError
from .tasklet import Tasklet, TaskletSettings, output_type, positional_parameters

__all__ = ["LoopMode", "Operator", "RollbackMode", "unbounded_operator", "bounded_operator"]

LoopMode = Literal["once", "per_input", "chained"]
"""How a bounded iterates: one pass, one per message, or each pass seeding the next."""

RollbackMode = Literal["all_or_nothing", "keep_progress"]
"""What a failure undoes: every completed tasklet, or the in-flight iteration only."""

#: The bounded facts a bounded class declares beside its channels.
_BOUNDED_KEYS = ("loop_mode", "rollback_mode", "max_duration_ms")

#: The one class setting a short form keeps: a loop is still one tasklet.
_SHORT_FORM_SETTINGS = ("loop_mode",)


class Operator:
    """One declared operator class, resolved to its spec.

    Registering performs no I/O: the decorator records the declaration and the
    spec is built on first use, so importing a declaring module opens no socket
    and starts no thread.
    """

    __slots__ = (
        "factory_id",
        "kind",
        "declaration",
        "tasklet",
        "_options",
        "_spec",
        "__name__",
        "doc",
    )

    def __init__(
        self,
        factory_id: str,
        kind: str,
        declaration: Callable[..., Any],
        options: dict[str, Any],
        tasklet: Tasklet[Any, Any] | None = None,
    ) -> None:
        self.factory_id = factory_id
        self.kind = kind
        self.declaration = declaration
        #: A short form's generated tasklet, named after the factory id and
        #: wireable in another operator's body; ``None`` for a channels body.
        self.tasklet = tasklet
        self._options = options
        self._spec: ClassSpec | None = None
        self.__name__ = getattr(declaration, "__name__", factory_id)
        #: The declaration's own docstring. Not `__doc__`, which `__slots__`
        #: may not name on a class that has one of its own.
        self.doc = declaration.__doc__

    def __repr__(self) -> str:
        return f"<flezha {self.kind} operator {self.factory_id}>"

    @property
    def spec(self) -> ClassSpec:
        if self._spec is None:
            self._spec = self._build_spec()
        return self._spec

    def _build_spec(self) -> ClassSpec:
        """Every flag the builder recorded, for either kind: the core judges which
        a kind may carry and refuses the rest by name."""
        builder = Channels(self.factory_id)
        self.declaration(builder)
        options = self._options
        sleep_when_idle = options["sleep_when_idle"]
        return ClassSpec(
            factory_id=self.factory_id,
            kind=self.kind,
            persistent=options["persistent"],
            tasklets=builder.tasklets,
            channels=builder.channels,
            sleep_when_idle=(
                self.kind == "bounded" and options["persistent"]
                if sleep_when_idle is None
                else sleep_when_idle
            ),
            effectively_once=builder.effectively_once_flag,
            checkpoint_ack=builder.checkpoint_ack_flag,
            checkpoint_every=builder.checkpoint_every_value,
            checkpoint_outputs=builder.checkpoint_outputs_value,
            checkpoint_max_delay_ms=builder.checkpoint_max_delay_ms_value,
            correction_window_ms=builder.correction_window_ms_value,
            beyond_window_policy=builder.beyond_window_policy_value,
            compact_after_bytes=builder.compact_after_bytes_value,
            sync_durability=options["sync_durability"],
            bounded=(
                {key: options[key] for key in _BOUNDED_KEYS} if self.kind == "bounded" else None
            ),
        )


def _declares_channels(declaration: Any) -> bool:
    if inspect.isclass(declaration):
        return False
    names = positional_parameters(declaration, skip_self=False)
    if len(names) != 1:
        return False
    annotation = _codec.type_hints(declaration).get(names[0])
    if isinstance(annotation, str):
        # Unresolvable where it was written (a function-local import under
        # postponed evaluation), so read by the name it spells.
        return annotation.rpartition(".")[2] == Channels.__name__
    return annotation is None or (isinstance(annotation, type) and issubclass(annotation, Channels))


def _operator(
    decorator: Callable[..., Any],
    factory_id: str | None,
    kind: str,
    options: dict[str, Any],
    short_form: dict[str, Any],
    settings: TaskletSettings,
) -> Callable[[Any], Operator]:
    unknown = sorted(settings.keys() - TaskletSettings.__optional_keys__)
    if unknown:
        raise TypeError(f"{decorator.__name__}() got an unexpected keyword argument {unknown[0]!r}")
    parameters = inspect.signature(decorator, eval_str=True).parameters
    for key, value in options.items():
        # A keyword annotated with a `Literal` takes only the values it spells.
        annotation = parameters[key].annotation
        if get_origin(annotation) is Literal and value not in get_args(annotation):
            raise DeclarationError(
                factory_id or "<operator>",
                None,
                f"`{key}={value!r}` is not one of `{'`, `'.join(get_args(annotation))}`",
            )
    class_settings = [
        key
        for key, value in options.items()
        if value != parameters[key].default and key not in _SHORT_FORM_SETTINGS
    ]

    def decorate(declaration: Any) -> Operator:
        name = getattr(declaration, "__name__", "<declaration>")
        if factory_id is None:
            raise DeclarationError(
                "<operator>",
                name,
                "declares no `id`; a factory id is an address a user types, so it is "
                "written rather than derived from the declaration's name",
            )
        if not _declares_channels(declaration):
            return _short_form(
                factory_id, kind, declaration, options, class_settings, settings, **short_form
            )
        written = [key for key, value in short_form.items() if value is not None]
        written += list(settings)
        if written:
            raise DeclarationError(
                factory_id,
                name,
                f"`{'`, `'.join(written)}` describe(s) what a short form generates, and a "
                f"`flezha.Channels` body declares its own channels and tasklets. A function "
                f"whose one parameter is annotated `flezha.Channels`, or not at all, is a "
                f"channels body",
            )
        return Operator(factory_id, kind, declaration, options)

    return decorate


def _short_form(
    factory_id: str,
    kind: str,
    body: Any,
    options: dict[str, Any],
    class_settings: list[str],
    settings: TaskletSettings,
    *,
    sink: str | None,
    input: str | None,
    codec: _codec.ChannelCodec | None,
) -> Operator:
    """One tasklet body as a whole operator: its entry channel, the
    tasklet named after the factory id and declared with ``settings``, and
    the sink when one is written."""
    name = body.__name__
    if inspect.isclass(body) and hasattr(body, "run") and hasattr(body, "channels"):
        raise DeclarationError(
            factory_id,
            name,
            "ShortFormWithTopology: `run` declares this operator's one tasklet, whose "
            "topology is generated, and `channels` declares a topology of its own; keep "
            "one of the two bodies",
            refusals=["ShortFormWithTopology"],
        )
    if class_settings:
        raise DeclarationError(
            factory_id,
            name,
            f"writes `{'`, `'.join(class_settings)}`; a short form keeps every class setting "
            f"but the loop mode at its default, so a class setting is declared in the full "
            f"form, with a `flezha.Channels` body",
        )
    generated = Tasklet(body, name=factory_id, **settings)
    returned = output_type(generated)
    if sink is not None and (returned is None or returned is type(None)):
        raise DeclarationError(
            factory_id,
            name,
            "SinkWithoutOutput: a `sink` requires an output type to feed the plugin; annotate "
            "the body's return",
            refusals=["SinkWithoutOutput"],
        )

    def channels(c: Channels) -> None:
        edge = c.input(input or "input", codec=codec).into(generated)
        if sink is not None:
            edge.sink(sink)

    channels.__name__ = name
    channels.__doc__ = generated.doc
    return Operator(factory_id, kind, channels, options, generated)


def unbounded_operator(
    id: str | None = None,
    *,
    persistent: bool = True,
    sleep_when_idle: bool | None = None,
    sync_durability: bool = False,
    sink: str | None = None,
    input: str | None = None,
    codec: _codec.ChannelCodec | None = None,
    **settings: Unpack[TaskletSettings],
) -> Callable[[Any], Operator]:
    """Declare an unbounded operator: fed by the controller through entry channels.

    ``id`` is the factory id. ``persistent`` is whether change bytes are flushed
    to the per-cell WAL. ``sleep_when_idle`` is whether an idle instance is
    written out and dropped from memory until its next input, off unless
    written. ``sync_durability`` is a bounded operator's setting, reaching
    the core from here so that it is refused by name (``SyncDurabilityOnUnbounded``)
    rather than dropped.

    Over a tasklet body this is the short form: an entry channel named by
    ``input`` (``"input"`` when none is written) and decoded by ``codec``, the
    tasklet, and the ``sink`` plugin channel when one is written. ``settings`` are the
    tasklet's own declarations, as :func:`~flezha.tasklet.tasklet` takes
    them.
    """
    return _operator(
        unbounded_operator,
        id,
        "unbounded",
        {
            "persistent": persistent,
            "sleep_when_idle": sleep_when_idle,
            "sync_durability": sync_durability,
        },
        {"sink": sink, "input": input, "codec": codec},
        settings,
    )


def bounded_operator(
    id: str | None = None,
    *,
    persistent: bool = True,
    sleep_when_idle: bool | None = None,
    loop_mode: LoopMode = "once",
    rollback_mode: RollbackMode = "all_or_nothing",
    max_duration_ms: int | None = None,
    sync_durability: bool = False,
    sink: str | None = None,
    input: str | None = None,
    codec: _codec.ChannelCodec | None = None,
    **settings: Unpack[TaskletSettings],
) -> Callable[[Any], Operator]:
    """Declare a bounded operator: a chain of tasklets, run once or looping.

    Retry is the edges', as on an unbounded operator: ``retry_max`` and ``retry_backoff_ms``
    on the channel feeding a tasklet. ``loop_mode`` and ``rollback_mode`` outside
    their declared values are refused here. ``sleep_when_idle`` defaults to
    ``persistent``: a bounded operator that keeps its state sleeps while it waits.

    Over a tasklet body this is the short form: a ``once`` bounded that runs
    its tasklet on its first message, or the ``loop_mode`` written, the one
    class setting a short form keeps; see :func:`unbounded_operator`.
    """
    return _operator(
        bounded_operator,
        id,
        "bounded",
        {
            "persistent": persistent,
            "sleep_when_idle": sleep_when_idle,
            "loop_mode": loop_mode,
            "rollback_mode": rollback_mode,
            "max_duration_ms": max_duration_ms,
            "sync_durability": sync_durability,
        },
        {"sink": sink, "input": input, "codec": codec},
        settings,
    )
