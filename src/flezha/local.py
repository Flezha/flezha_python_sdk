"""Running a declared relay graph in this process, with no controller.

:meth:`LocalRelay.start` takes the relay you deploy and runs the
``channels`` graph it already declares — no local-mode member on the class, no
second declaration. What runs is the deployed machinery: entry decode, edge
transforms and keying, one Cell per resolved key with its own mailbox, the
retry ladder, and inter-tasklet delivery performed by the runtime.

State is held in memory and nothing is durable. Durability, plugins, placement
and migration, the dead-letter queue, observability export, authz and
multi-runner scale all need a controller; the run says so through the SDK's
logger every time it starts. The "Local mode" section of
https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#local-mode
walks through it.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Self

from . import graph
from .dispatch import DispatchDeps, Registry
from .errors import FlezhaError
from .executor import build as build_executor
from .native import Core
from .relay import Relay

__all__ = ["LocalRelay", "LocalOutput"]

#: Bounded so a stopped run's drain thread leaves promptly.
_OUTPUT_POLL_MS = 100


@dataclass(slots=True)
class LocalOutput:
    """One value a graph released on a declared output."""

    channel: str
    payload: bytes
    #: What a deployed run would have put on the wire, ``flz.*`` stamps included.
    metadata: dict[str, str] = field(default_factory=dict)


class LocalRelay:
    """One relay graph running in this process."""

    __slots__ = ("_core", "_executor", "_sinks", "_drain", "_closed")

    def __init__(self, core: Core, executor: Any) -> None:
        self._core = core
        self._executor = executor
        self._sinks: dict[str, Callable[[LocalOutput], None]] = {}
        self._closed = threading.Event()
        self._drain = threading.Thread(
            target=self._drain_outputs, name="flz-local-output", daemon=True
        )
        self._drain.start()

    @classmethod
    def start(cls, relay: Relay) -> LocalRelay:
        """Register ``relay``'s declared graph and start it locally.

        Raises naming the capability when the declaration needs a controller.
        """
        core = Core.create()
        try:
            spec = relay.spec
            registry = Registry()
            graph.declare(spec, core, registry)
            # Not `start`, which is the one call that dials a controller.
            core.local_start(spec.factory_id, f"{spec.factory_id}-local")
            deps = DispatchDeps(registry=registry, bridge=core)
            executor = build_executor(None, core, deps)
            executor.start()
        except BaseException:
            core.free()
            raise
        return cls(core, executor)

    def on_output(self, channel: str, callback: Callable[[LocalOutput], None]) -> Self:
        """Deliver every value released on ``channel`` to ``callback``.

        An output channel takes the producing tasklet's name unless the
        declaration named it.
        """
        # Registered with the core first: a channel the declaration does not
        # carry is refused there, naming it, rather than silently accepted.
        self._core.local_on_sink(channel)
        self._sinks[channel] = callback
        return self

    def feed(self, channel: str, payload: bytes | str) -> Self:
        """Feed one value's wire bytes to a declared entry channel."""
        self._core.local_feed(channel, payload.encode() if isinstance(payload, str) else payload)
        return self

    def run_until_idle(self, budget_ms: int) -> None:
        """Block until the graph has nothing left to do.

        A budget that elapses first is reported, never passed off as done.
        """
        self._core.local_run_until_idle(budget_ms)

    def close(self) -> None:
        """End the run and release its Cells. Idempotent."""
        if self._closed.is_set():
            return
        self._closed.set()
        self._drain.join(timeout=_OUTPUT_POLL_MS / 1000 * 20)
        # Stop the run first, then take what it released and the drain thread
        # had not reached: a value can be queued for up to one poll interval,
        # and ending the run while it sits there is how an author loses output
        # the graph did produce. Stopping drops the sender, so this ends at the
        # first empty read rather than waiting.
        self._core.local_stop()
        while (output := self._core.local_next_output(0)) is not None:
            self._deliver(output)
        # Not `Core.shutdown`: that stops the process-wide poll core, and a
        # process may start another local run after this one. The workers
        # leave on the flag instead, which is why it is raised before the
        # drain rather than after it.
        self._executor.stop_fetching()
        undrained = self._executor.stop()
        # As the runner does: a worker the bounded drain gave up on may yet
        # call the core, and freeing the context under it is a use-after-free.
        if undrained == 0:
            self._core.free()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def _drain_outputs(self) -> None:
        """One released value per pass, handed to whoever registered its channel."""
        while not self._closed.is_set():
            try:
                output = self._core.local_next_output(_OUTPUT_POLL_MS)
            except FlezhaError as error:
                # The run ended under us; the close path owns the teardown. A
                # forked process is a defect, not a stop.
                if error.code == "runtime.forked_process":
                    raise
                return
            if output is not None:
                self._deliver(output)

    def _deliver(self, output: dict[str, Any]) -> None:
        if output.get("target") != "plugin":
            # The core queues what an output released and nothing else; an edge
            # output is an in-process mailbox push that never leaves.
            raise RuntimeError(f"local mode released a {output.get('target')} output")
        channel = str(output["plugin_channel"])
        callback = self._sinks.get(channel)
        if callback is None:
            return
        payload = bytes(output["payload"])
        callback(LocalOutput(channel, payload, {k: v for k, v in output.get("metadata", ())}))
