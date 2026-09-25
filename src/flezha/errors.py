"""Everything this SDK raises, so a host can catch one thing."""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

__all__ = [
    "FlezhaError",
    "CoreError",
    "SchemaMismatchError",
    "DeclarationError",
    "EmitError",
    "UnrepresentableValueError",
    "NonRetryableError",
    "ForkError",
    "SpawnError",
    "SpawnAlreadyRunningError",
    "SpawnFactoryUnknownError",
    "SpawnFactoryNotBoundedError",
    "SpawnRefusedError",
    "SpawnTimeoutError",
    "SpawnUnavailableError",
    "DeterminismWarning",
]


class FlezhaError(Exception):
    """Base of every error this SDK raises."""


class CoreError(FlezhaError):
    """A failure the native core reported, with its own detail preserved.

    The veneer raises the extension's own exception type; :class:`~flezha.native.Core`
    translates it here so a host catches one thing, and chains the original so
    nothing about the engine's answer is lost.
    """

    def __init__(self, message: str, verdict: bytes | None = None) -> None:
        super().__init__(message)
        #: The refusal as the core's own JSON, when the entry point answered one
        #: beside the rendered message — the topology check does. ``None``
        #: everywhere else.
        self.verdict = verdict


class SchemaMismatchError(CoreError):
    """The native library and this package speak different activation schemas.

    Raised at context creation, naming both versions, before any controller
    connection is attempted.
    """


class DeclarationError(FlezhaError):
    """A registration refusal: the class, the handler and the defect.

    Registration-time refusal is what Python has in place of the Rust SDK's
    compile-time one for the defects a type checker cannot see — a cycle, a
    missing entry channel, an unservable clock — so the message carries what a
    compiler would have pointed at.
    """

    def __init__(
        self,
        factory_id: str,
        handler: str | None,
        defect: str,
        refusals: list[str] | None = None,
    ) -> None:
        self.factory_id = factory_id
        self.handler = handler
        self.defect = defect
        #: The core's vocabulary names for what was refused: the core's verdict,
        #: or a refusal this SDK makes under one of them. Empty for a refusal of
        #: this SDK's own surface.
        self.refusals = refusals or []
        # A named refusal reads the way the core renders its verdict. The
        # core's own verdict arrives already rendered, header included.
        header = f"topology rejected [{', '.join(self.refusals)}]:"
        if self.refusals and not defect.startswith(header):
            defect = f"{header}\n  - {defect}"
        super().__init__(
            f"{factory_id}: {handler}: {defect}" if handler else f"{factory_id}: {defect}"
        )


class EmitError(FlezhaError):
    """An emit the declaration cannot carry.

    A channel the tasklet does not produce on, a reserved metadata key, a
    keyed channel with no key, a timer inside a bounded operator.
    """


class UnrepresentableValueError(FlezhaError):
    """A value the durable codec cannot represent, named by type and field."""

    def __init__(self, type_name: str, path: str, reason: str) -> None:
        self.type_name = type_name
        self.path = path
        super().__init__(f"{type_name}.{path} cannot be encoded for durable state: {reason}")


class NonRetryableError(FlezhaError):
    """A handler error the retry ladder must not retry.

    Raise this — or set ``non_retryable = True`` on any exception — to park an
    item immediately instead of consuming its remaining budget.
    """

    non_retryable = True


class ForkError(FlezhaError):
    """A core call from a process that did not start the core."""


class SpawnError(FlezhaError):
    """Why a spawn reached no instance.

    Raised by a read of the :class:`~flezha.ctx.SpawnHandle`, never by
    ``ctx.spawn_bounded`` itself, so a refusal faults the tasklet only when
    it asks. Each kind is a subclass; ``reason`` is its word on the wire.
    """

    reason: ClassVar[str] = ""

    def __init__(self, message: str, id: str = "") -> None:
        super().__init__(message)
        #: The id the request named; empty when it asked for a generated one.
        self.id = id

    @staticmethod
    def of(answer: Mapping[str, str]) -> SpawnError:
        """The error a failed spawn answer names, as the core renders it."""
        kinds = {kind.reason: kind for kind in SpawnError.__subclasses__()}
        return kinds.get(answer["error"], SpawnError)(answer["message"], answer["id"])


class SpawnAlreadyRunningError(SpawnError):
    """The id is taken and the request carried ``if_running="error"``."""

    reason = "already_running"


class SpawnFactoryUnknownError(SpawnError):
    """No connected runner registers the requested factory."""

    reason = "factory_unknown"


class SpawnFactoryNotBoundedError(SpawnError):
    """The requested factory is registered as unbounded: only a bounded operator spawns."""

    reason = "factory_not_bounded"


class SpawnRefusedError(SpawnError):
    """The controller declined: authorization, quota, or a failed start."""

    reason = "refused"


class SpawnTimeoutError(SpawnError):
    """No answer arrived within the core's deadline for the request."""

    reason = "timeout"


class SpawnUnavailableError(SpawnError):
    """No control plane to request a spawn on."""

    reason = "unavailable"


class DeterminismWarning(UserWarning):
    """An ambient clock or identity read inside an invocation.

    A warning and not an error: the call is legal Python and the invocation
    completes, but its value differs between the live run and the replay, which
    is a defect nothing else reports.
    """
