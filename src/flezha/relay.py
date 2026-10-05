"""The relay decorator: what an author declares and what a runner registers.

A relay is its channel graph, so the decorated object is the function that
builds one:

.. code-block:: python

    @flezha.relay("echo")
    def echo(c: flezha.Channels) -> None:
        c.input("input").into(echo_task).output("out")

A channels body takes one parameter, annotated :class:`~flezha.builder.Channels`
or not at all. The class settings are keywords of the decorator: ``run`` (a
flow, the default, or steps), ``durable``, ``max_duration_ms`` and
``description``.

Handed a tasklet body instead, the decorator declares the single-tasklet short
form: a function whose one parameter is the input and whose return value is
emitted, or a tasklet class, which emits through its context:

.. code-block:: python

    @flezha.relay(id="echo", output="out", codec="text")
    def echo(value: str) -> str:
        return value
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Literal, TypedDict, Unpack

from . import codec as _codec
from .builder import Channels, ClassSpec
from .errors import FlezhaError
from .tasklet import Tasklet, TaskletSettings, output_type, positional_parameters

__all__ = [
    "Ack",
    "Durable",
    "Each",
    "Exactly",
    "OnFailure",
    "Relay",
    "Run",
    "relay",
]

Each = Literal["once", "per_input", "chained"]
"""How a steps run iterates: one pass, one per message, or each pass seeding the next."""

OnFailure = Literal["undo_all", "keep_progress"]
"""What a failure undoes: every completed tasklet, or the in-flight iteration only."""


class StepsSettings(TypedDict, total=False):
    each: Each
    on_failure: OnFailure


class CheckpointSettings(TypedDict, total=False):
    every: int
    max_delay_ms: int
    outputs: Literal["as_produced", "at_checkpoint"]


class _Checkpoint(TypedDict):
    checkpoint: CheckpointSettings


Ack = Literal["per_input", "checkpoint"] | _Checkpoint
"""When a flow acknowledges an input: one by one, or at the checkpoint."""


class EffectivelyOnceSettings(TypedDict, total=False):
    ack: Ack
    correction_ms: int
    beyond_window: Literal["process_as_arrival", "drop"]


class _EffectivelyOnce(TypedDict):
    effectively_once: EffectivelyOnceSettings


Exactly = Literal["at_least_once", "effectively_once"] | _EffectivelyOnce
"""A flow's duplicate rule."""


class FlowSettings(TypedDict, total=False):
    exactly: Exactly


class _Flow(TypedDict):
    flow: FlowSettings


class _Steps(TypedDict):
    steps: StepsSettings


Run = Literal["flow", "steps"] | _Flow | _Steps
"""How the class runs: a flow (the default) or in steps, bare or with its members."""


class KeptSettings(TypedDict, total=False):
    sleep_after: int | Literal[False]


class _Local(TypedDict):
    local: KeptSettings


class _Remote(TypedDict):
    remote: KeptSettings


Durable = Literal[False, "local", "remote"] | _Local | _Remote
"""Where an instance keeps its work. Unless ``sleep_after`` names an interval in
milliseconds, or ``False``, a kept flow instance never sleeps and a kept steps
instance sleeps after six idle hours."""


class Relay:
    """One declared relay class, resolved to its spec.

    Registering performs no I/O: the decorator records the declaration and the
    spec is built on first use, so importing a declaring module opens no socket
    and starts no thread.
    """

    __slots__ = (
        "factory_id",
        "declaration",
        "tasklet",
        "_settings",
        "_spec",
        "__name__",
        "doc",
    )

    def __init__(
        self,
        factory_id: str,
        declaration: Callable[..., Any],
        settings: dict[str, Any],
        tasklet: Tasklet[Any, Any] | None = None,
    ) -> None:
        self.factory_id = factory_id
        self.declaration = declaration
        #: A short form's generated tasklet, named after the factory id and
        #: wireable in another relay's body; ``None`` for a channels body.
        self.tasklet = tasklet
        self._settings = settings
        self._spec: ClassSpec | None = None
        self.__name__ = getattr(declaration, "__name__", factory_id)
        #: The declaration's own docstring. Not `__doc__`, which `__slots__`
        #: may not name on a class that has one of its own.
        self.doc = declaration.__doc__

    def __repr__(self) -> str:
        return f"<flezha relay {self.factory_id}>"

    @property
    def spec(self) -> ClassSpec:
        if self._spec is None:
            self._spec = self._build_spec()
        return self._spec

    def _build_spec(self) -> ClassSpec:
        """The graph and the settings tree as written: the core judges the tree
        and refuses a misplaced setting by name."""
        builder = Channels(self.factory_id)
        self.declaration(builder)
        return ClassSpec(
            factory_id=self.factory_id,
            tasklets=builder.tasklets,
            channels=builder.channels,
            compact_after_bytes=builder.compact_after_bytes_value,
            **self._settings,
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


def relay(
    id: str | None = None,
    *,
    run: Run | None = None,
    durable: Durable | None = None,
    max_duration_ms: int | None = None,
    description: str | None = None,
    output: str | None = None,
    input: str | None = None,
    codec: _codec.ChannelCodec | None = None,
    **settings: Unpack[TaskletSettings],
) -> Callable[[Any], Relay]:
    """Declare a relay: fed by the controller through entry channels.

    ``id`` is the factory id. ``run`` is ``"flow"`` (the default) or
    ``"steps"``, bare or with its members, e.g.
    ``{"steps": {"each": "per_input"}}``; ``durable`` is ``False``, ``"local"``
    (the default) or ``"remote"``, e.g. ``{"local": {"sleep_after": 2_000}}``.
    The core reads both when the class registers and refuses a setting under a
    parent that does not carry it as ``topology.unknown_field``.

    Over a tasklet body this is the short form, with the same class settings:
    an entry channel named by ``input`` (``"input"`` when none is written) and
    decoded by ``codec``, the tasklet, and the ``output`` plugin channel when one
    is written. ``settings`` are the tasklet's own declarations, as
    :func:`~flezha.tasklet.tasklet` takes them.
    """
    unknown = sorted(settings.keys() - TaskletSettings.__optional_keys__)
    if unknown:
        raise TypeError(f"relay() got an unexpected keyword argument {unknown[0]!r}")
    class_settings = {
        "run": run,
        "durable": durable,
        "max_duration_ms": max_duration_ms,
        "description": description,
    }

    def decorate(declaration: Any) -> Relay:
        name = getattr(declaration, "__name__", "<declaration>")
        if id is None:
            # A factory id is an address a user types, so it is written
            # rather than derived from the declaration's name.
            raise FlezhaError("topology.missing_factory_id", {"relay": name})
        if not _declares_channels(declaration):
            return _short_form(
                id, declaration, class_settings, settings, output=output, input=input, codec=codec
            )
        written = [
            key
            for key, value in {"output": output, "input": input, "codec": codec}.items()
            if value is not None
        ]
        written += list(settings)
        if written:
            raise FlezhaError(
                "topology.short_form_field_on_full_form",
                {"factory_id": id, "fields": [f"`{key}`" for key in written]},
            )
        return Relay(id, declaration, class_settings)

    return decorate


def _short_form(
    factory_id: str,
    body: Any,
    class_settings: dict[str, Any],
    settings: TaskletSettings,
    *,
    output: str | None,
    input: str | None,
    codec: _codec.ChannelCodec | None,
) -> Relay:
    """One tasklet body as a whole relay: its entry channel, the
    tasklet named after the factory id and declared with ``settings``, and
    the output when one is written."""
    name = body.__name__
    if inspect.isclass(body) and hasattr(body, "run") and hasattr(body, "channels"):
        raise FlezhaError("topology.short_form_with_topology", {"factory_id": factory_id})
    generated = Tasklet(body, name=factory_id, **settings)
    returned = output_type(generated)
    # An unresolvable hint degrades to the raw annotation, where `None` is the string.
    if output is not None and (returned is None or returned is type(None) or returned == "None"):
        raise FlezhaError("topology.output_without_type", {"factory_id": factory_id})

    def channels(c: Channels) -> None:
        edge = c.input(input or "input", codec=codec).into(generated)
        if output is not None:
            edge.output(output)

    channels.__name__ = name
    channels.__doc__ = generated.doc
    return Relay(factory_id, channels, class_settings, generated)
