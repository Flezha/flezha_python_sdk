"""The operative decorators: what an author declares and what a runner registers.

An operative is its channel graph, so the decorated object is the function that
builds one:

.. code-block:: python

    @flezha.stream_operative("echo")
    def echo(c: flezha.Channels) -> None:
        c.input("input").into(echo_task).sink("out")

A workflow operative is the same shape: a chain of tasklets over a graph. A
channels body takes one parameter, annotated :class:`~flezha.builder.Channels` or
not at all.

Handed a tasklet body instead, a class with ``run`` or a function of any
other shape, the same decorators declare the single-tasklet short form:

.. code-block:: python

    @flezha.stream_operative(id="echo", sink="out", codec="text")
    def echo(value: str) -> str:
        return value
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Unpack

from . import codec as _codec
from .builder import Channels, ClassSpec
from .errors import DeclarationError
from .tasklet import Tasklet, TaskletSettings, output_type, positional_parameters

__all__ = ["Operative", "stream_operative", "workflow_operative"]


class Operative:
    """One declared operative class, resolved to its spec.

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
        #: wireable in another operative's body; ``None`` for a channels body.
        self.tasklet = tasklet
        self._options = options
        self._spec: ClassSpec | None = None
        self.__name__ = getattr(declaration, "__name__", factory_id)
        #: The declaration's own docstring. Not `__doc__`, which `__slots__`
        #: may not name on a class that has one of its own.
        self.doc = declaration.__doc__

    def __repr__(self) -> str:
        return f"<flezha {self.kind} operative {self.factory_id}>"

    @property
    def spec(self) -> ClassSpec:
        if self._spec is None:
            self._spec = self._build_spec()
        return self._spec

    def _build_spec(self) -> ClassSpec:
        return self._stream_spec() if self.kind == "stream" else self._workflow_spec()

    def _channels(self) -> Channels:
        builder = Channels(self.factory_id)
        self.declaration(builder)
        return builder

    def _stream_spec(self) -> ClassSpec:
        builder = self._channels()
        return ClassSpec(
            factory_id=self.factory_id,
            kind="stream",
            persistent=self._options["persistent"],
            tasklets=builder.tasklets,
            channels=builder.channels,
            effectively_once=builder.effectively_once_flag,
            checkpoint_ack=builder.checkpoint_ack_flag,
            checkpoint_every=builder.checkpoint_every_value,
            checkpoint_outputs=builder.checkpoint_outputs_value,
            checkpoint_max_delay_ms=builder.checkpoint_max_delay_ms_value,
            correction_window_ms=builder.correction_window_ms_value,
            beyond_window_policy=builder.beyond_window_policy_value,
            compact_after_bytes=builder.compact_after_bytes_value,
            sync_durability=builder.sync_durability_flag,
        )

    def _workflow_spec(self) -> ClassSpec:
        builder = self._channels()
        options = self._options
        return ClassSpec(
            factory_id=self.factory_id,
            kind="workflow",
            persistent=options["persistent"],
            tasklets=builder.tasklets,
            channels=builder.channels,
            effectively_once=builder.effectively_once_flag,
            sync_durability=bool(options["sync_durability"]) or builder.sync_durability_flag,
            workflow={
                "loop_mode": options["loop_mode"],
                "rollback_mode": options["rollback_mode"],
                "retry_attempts": options["retry_attempts"],
                "retry_backoff_ms": options["retry_backoff_ms"],
                "barrier_retry_attempts": options["barrier_retry_attempts"],
                "max_duration_ms": options["max_duration_ms"],
                "wake_after_ms": options["wake_after_ms"],
            },
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


def _operative(
    decorator: Callable[..., Any],
    factory_id: str | None,
    kind: str,
    options: dict[str, Any],
    short_form: dict[str, Any],
    settings: TaskletSettings,
) -> Callable[[Any], Operative]:
    unknown = sorted(settings.keys() - TaskletSettings.__optional_keys__)
    if unknown:
        raise TypeError(f"{decorator.__name__}() got an unexpected keyword argument {unknown[0]!r}")
    defaults = inspect.signature(decorator).parameters
    class_settings = [key for key, value in options.items() if value != defaults[key].default]

    def decorate(declaration: Any) -> Operative:
        name = getattr(declaration, "__name__", "<declaration>")
        if factory_id is None:
            raise DeclarationError(
                "<operative>",
                name,
                "declares no `id`; a factory id is an address an operator types, so it is "
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
        return Operative(factory_id, kind, declaration, options)

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
) -> Operative:
    """One tasklet body as a whole operative: its entry channel, the
    tasklet named after the factory id and declared with ``settings``, and
    the sink when one is written."""
    name = body.__name__
    topology = [key for key in ("channels", "tasklets") if hasattr(body, key)]
    if inspect.isclass(body) and hasattr(body, "run") and topology:
        raise DeclarationError(
            factory_id,
            name,
            f"ShortFormWithTopology: `run` declares this operative's one tasklet, whose "
            f"topology is generated, and `{topology[0]}` declares a topology of its own; keep "
            f"one of the two bodies",
            refusals=["ShortFormWithTopology"],
        )
    if class_settings:
        raise DeclarationError(
            factory_id,
            name,
            f"writes `{'`, `'.join(class_settings)}`; a short form keeps every class setting at "
            f"its default, so a class setting is declared in the full form, with a "
            f"`flezha.Channels` body",
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
    return Operative(factory_id, kind, channels, options, generated)


def stream_operative(
    id: str | None = None,
    *,
    persistent: bool = True,
    sink: str | None = None,
    input: str | None = None,
    codec: _codec.ChannelCodec | None = None,
    **settings: Unpack[TaskletSettings],
) -> Callable[[Any], Operative]:
    """Declare a stream operative: fed by the controller through entry channels.

    ``id`` is the factory id. ``persistent`` is whether change bytes are flushed
    to the per-cell WAL.

    Over a tasklet body this is the short form: an entry channel named by
    ``input`` (``"input"`` when none is written) and decoded by ``codec``, the
    tasklet, and the ``sink`` plugin channel when one is written. ``settings`` are the
    tasklet's own declarations, as :func:`~flezha.tasklet.tasklet` takes
    them.
    """
    return _operative(
        stream_operative,
        id,
        "stream",
        {"persistent": persistent},
        {"sink": sink, "input": input, "codec": codec},
        settings,
    )


def workflow_operative(
    id: str | None = None,
    *,
    persistent: bool = True,
    loop_mode: str = "once",
    rollback_mode: str = "all_or_nothing",
    retry_attempts: int | None = None,
    retry_backoff_ms: int | None = None,
    barrier_retry_attempts: int | None = None,
    max_duration_ms: int | None = None,
    wake_after_ms: int | None = None,
    sync_durability: bool = False,
    sink: str | None = None,
    input: str | None = None,
    codec: _codec.ChannelCodec | None = None,
    **settings: Unpack[TaskletSettings],
) -> Callable[[Any], Operative]:
    """Declare a workflow operative: a chain of tasklets, run once or looping.

    Over a tasklet body this is the short form, a ``once`` workflow that runs
    its tasklet on its first message; see :func:`stream_operative`.
    """
    return _operative(
        workflow_operative,
        id,
        "workflow",
        {
            "persistent": persistent,
            "loop_mode": loop_mode,
            "rollback_mode": rollback_mode,
            "retry_attempts": retry_attempts,
            "retry_backoff_ms": retry_backoff_ms,
            "barrier_retry_attempts": barrier_retry_attempts,
            "max_duration_ms": max_duration_ms,
            "wake_after_ms": wake_after_ms,
            "sync_durability": sync_durability,
        },
        {"sink": sink, "input": input, "codec": codec},
        settings,
    )
