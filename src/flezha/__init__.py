"""The Flezha Python SDK: a poll host over ``flz_core``.

The SDK contains no kernel code. Transport, framing, ordering, per-cell
exclusivity, durability, retries, the retry-to-park ladder, timers, credit,
routing, the steps driver and reconnect all live in the core; this package
brings the authoring surface, the codec for user types, the threading host and
idiomatic errors.

.. code-block:: python

    import flezha

    @flezha.tasklet(name="echo")
    class Echo:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

    @flezha.relay("echo")
    def echo_relay(c: flezha.Channels) -> None:
        c.input("input").into(Echo).output("out")

    flezha.create_runner("default").register_factory(echo_relay).run()
"""

from __future__ import annotations

from .builder import DROPPED, Channels, Edge, Merge
from .call import Response
from .cell_state import state
from .codec import ChannelCodec, TypeCodec, ValueCodec
from .control import Control, StartResult
from .ctx import Ctx, SpawnHandle, SpawnResult
from .errors import DeterminismWarning, FlezhaError, NonRetryableError
from .local import LocalOutput, LocalRelay
from .logger import configure as configure_logging
from .relay import Durable, Relay, Run, relay
from .runner import ACTIVATION_SCHEMA_VERSION, Runner, create_runner
from .tasklet import Tasklet, tasklet
from .version import __version__

__all__ = [
    "Control",
    "StartResult",
    # authoring
    "tasklet",
    "state",
    "relay",
    "Channels",
    "Edge",
    "Merge",
    "Response",
    "Ctx",
    "SpawnHandle",
    "SpawnResult",
    "Tasklet",
    "Relay",
    "Run",
    "Durable",
    "DROPPED",
    # runner
    "create_runner",
    "Runner",
    "LocalRelay",
    "LocalOutput",
    "configure_logging",
    # codec
    "ChannelCodec",
    "TypeCodec",
    "ValueCodec",
    # errors
    "FlezhaError",
    "NonRetryableError",
    "DeterminismWarning",
    # versions
    "__version__",
    "ACTIVATION_SCHEMA_VERSION",
]
