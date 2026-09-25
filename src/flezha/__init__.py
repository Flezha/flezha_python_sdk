"""The Flezha Python SDK: a poll host over ``flz_core``.

The SDK contains no kernel code. Transport, framing, ordering, per-cell
exclusivity, durability, retries, the retry-to-park ladder, timers, credit,
routing, the bounded driver and reconnect all live in the core; this package
brings the authoring surface, the codec for user types, the threading host and
idiomatic errors.

.. code-block:: python

    import flezha

    @flezha.tasklet
    def echo(value: str) -> str:
        return value

    @flezha.unbounded_operator("echo")
    def echo_operator(c: flezha.Channels) -> None:
        c.input("input").into(echo).sink("out")

    flezha.create_runner("default").register_factory(echo_operator).run()
"""

from __future__ import annotations

from .admin import Admin, StartResult
from .builder import DROPPED, Channels, Edge, Merge
from .cell_state import state
from .codec import ChannelCodec, TypeCodec, ValueCodec
from .ctx import Ctx, SpawnHandle, SpawnResult
from .errors import (
    CoreError,
    DeclarationError,
    DeterminismWarning,
    EmitError,
    FlezhaError,
    ForkError,
    NonRetryableError,
    SchemaMismatchError,
    SpawnAlreadyRunningError,
    SpawnError,
    SpawnFactoryNotBoundedError,
    SpawnFactoryUnknownError,
    SpawnRefusedError,
    SpawnTimeoutError,
    SpawnUnavailableError,
    UnrepresentableValueError,
)
from .local import LocalOperator, SinkOutput
from .logger import configure as configure_logging
from .operator import Operator, bounded_operator, unbounded_operator
from .runner import ACTIVATION_SCHEMA_VERSION, Runner, create_runner
from .tasklet import Tasklet, tasklet
from .version import __version__

__all__ = [
    "Admin",
    "StartResult",
    # authoring
    "tasklet",
    "state",
    "unbounded_operator",
    "bounded_operator",
    "Channels",
    "Edge",
    "Merge",
    "Ctx",
    "SpawnHandle",
    "SpawnResult",
    "Tasklet",
    "Operator",
    "DROPPED",
    # runner
    "create_runner",
    "Runner",
    "LocalOperator",
    "SinkOutput",
    "configure_logging",
    # codec
    "ChannelCodec",
    "TypeCodec",
    "ValueCodec",
    # errors
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
    # versions
    "__version__",
    "ACTIVATION_SCHEMA_VERSION",
]
