"""The runner: what an author starts, and everything it owns while running.

Classes are registered explicitly, by naming them or the modules that declare
them. There is no autoload, no path walk and no package scan: what runs is what
the author said runs, and a class that is defined but not registered never
reaches the core.

Startup order::

    declare classes -> (fork, if the user forks at all) -> start

The core must NOT be started before a fork: it owns native threads that
``fork(2)`` does not carry, and :meth:`Core._call <flezha.native.Core._call>`
raises naming that rule rather than letting a child serve nothing.
"""

from __future__ import annotations

import importlib
import os
import sys
import threading
from types import TracebackType
from typing import Any, Self

from . import determinism, graph
from .admin import Admin
from .dispatch import DispatchDeps, Registry
from .errors import FlezhaError
from .executor import Executor
from .executor import build as build_executor
from .logger import logger
from .native import ACTIVATION_SCHEMA_VERSION, Core, build_tier
from .operator import Operator

__all__ = ["Runner", "create_runner", "ACTIVATION_SCHEMA_VERSION"]

_log = logger("runner")

# What a healthy runner has to drain in after standard input closes.
_STDIN_EOF_GRACE_MS = 5_000

#: Only one runner may be *started* per process: a runner is a transport
#: connection and a connection lives in one process. Any number of unstarted
#: runners may exist.
_started_slot = threading.Lock()


def _operator_of(value: object) -> Operator:
    """The operator a registration call was handed.

    A tasklet declares none, and saying so here is what keeps the refusal at
    the call the author wrote.
    """
    if not isinstance(value, Operator):
        raise FlezhaError(
            "topology.no_operator_exported", {"module": str(getattr(value, "__name__", value))}
        )
    return value


class Runner:
    """One runner: its namespace, its declared types and its executor."""

    def __init__(self, namespace: str) -> None:
        self._namespace = namespace
        self._operators: list[Operator] = []
        self._registry = Registry()
        self._core: Core | None = None
        self._executor: Executor | None = None
        self._executor_kind = os.environ.get("FLEZHA_PYTHON_EXECUTOR", "thread")
        self._executor_options: dict[str, Any] = {}
        self._started = False
        self._stopped = threading.Event()
        self._teardown: list[Any] = []
        self._holds_slot = False

    # -- declaration -------------------------------------------------------

    @property
    def namespace(self) -> str:
        return self._namespace

    def operators(self) -> list[str]:
        """The factory ids this runner currently declares."""
        return self._registry.ids() if self._started else [o.factory_id for o in self._operators]

    def register_factory(self, operator: Operator) -> Runner:
        """Register an operator type of either kind, short form or full.

        The kind is read from what is handed in, never named by the call.
        Legal both before and after :meth:`start`; a post-start declaration
        reaches the core immediately and the controller at the next bind.
        """
        return self._define(_operator_of(operator))

    def register_modules(self, *modules: str) -> Runner:
        """Import the named modules and register everything they declare.

        Every :class:`~flezha.operator.Operator` the module holds, short form
        or full. No implicit package scanning and no filesystem discovery: the
        modules are named, and one that cannot be imported raises its own
        :class:`ImportError`.
        """
        for name in modules:
            module = importlib.import_module(name)
            for value in vars(module).values():
                if isinstance(value, Operator):
                    self._define(value)
        return self

    def unregister_factory(self, operator: Operator) -> Runner:
        """Undeclare a type — the inverse of :meth:`register_factory`, taking what it took.

        Refuses one with live instances, and one never declared.
        """
        factory_id = _operator_of(operator).factory_id
        if not any(o.factory_id == factory_id for o in self._operators):
            raise FlezhaError("runtime.class_not_registered", {"factory": factory_id})
        if self._started:
            registered = self._registry.get(factory_id)
            live = 0 if registered is None else registered.live_instances()
            if live:
                raise FlezhaError(
                    "runtime.operator_type_in_use",
                    {"factory_id": factory_id, "live_instances": live},
                )
            if registered is not None:
                self._registry.remove(factory_id)
            # The core keeps its own declaration until the context ends: the
            # ABI has no entry point that retracts one, so this runner stops
            # serving the type and the controller learns of the removal at the
            # next bind, whose init frame is serialized from the core's set.
            _log.debug(
                "the removal of `%s` has no core surface; this runner stops serving it and "
                "the declaration lapses when the context ends",
                factory_id,
            )
        self._operators = [o for o in self._operators if o.factory_id != factory_id]
        return self

    def executor(self, kind: str, **options: Any) -> Runner:
        """Which executor dispatches: ``thread`` (the default) or ``asyncio``."""
        self._executor_kind = kind
        self._executor_options = options
        return self

    def admin(self) -> Admin:
        """Administer this namespace from ordinary application code.

        Start, stop and feed operators, enable and disable plugins, read what
        is running. The controller judges every call as the identity this
        connection proved, so no credential is held here and nothing expires.
        A user grants a namespace's applications the authority once, with
        ``flz role bind runner@<namespace> app <namespace>``; until then every
        call is refused. It needs a started runner, whose connection is what
        the controller judges.
        """
        if self._core is None:
            raise FlezhaError("runtime.runner_not_started")
        return Admin(self._core)

    # -- lifecycle ---------------------------------------------------------

    def start(self, *, core: Core | None = None) -> Self:
        """Declare, connect and start serving.

        Everything up to and including the first poll, and nothing that blocks.
        Separate from :meth:`run` so a host with its own supervision tree can
        own the waiting, and so the startup order is something a test can
        observe rather than a comment.
        """
        self._claim_slot()
        try:
            # Before anything else: a native library built against a different
            # activation schema is refused here, with no controller connection
            # attempted.
            self._core = core or Core.create()
            determinism.install()
            for operator in self._operators:
                self._declare(operator)
            self._core.start(self._namespace)
            self._observe_shutdown()

            self._executor = self._build_executor()
            self._executor.start()
            self._started = True
        except BaseException:
            self._release_slot()
            raise

        _log.info(
            "runner up on namespace %s: %s tier, %s",
            self._namespace,
            build_tier(),
            self._executor.describe(),
        )
        return self

    def _claim_slot(self) -> None:
        """One runner per process: a runner is one transport connection."""
        if self._started:
            raise FlezhaError("runtime.runner_already_running")
        # A runner is one transport connection, so N processes are N runners.
        if not _started_slot.acquire(blocking=False):
            raise FlezhaError("runtime.runner_already_running")
        self._holds_slot = True

    def _build_executor(self) -> Executor:
        deps = DispatchDeps(registry=self._registry, bridge=self._core)
        return build_executor(
            self._executor_kind,
            self._core,
            deps,
            on_core_shutdown=self.stop,
            **self._executor_options,
        )

    def await_termination(self) -> Runner:
        """Block until the runner is stopped, then drain."""
        self._stopped.wait()
        self.drain()
        return self

    def run(self, *, core: Core | None = None) -> Runner:
        """Declare, connect and serve until shutdown.

        Sugar for :meth:`start` followed by :meth:`await_termination`, never an
        independent implementation.
        """
        return self.start(core=core).await_termination()

    def stop(self) -> None:
        """Stop fetching and drain what is in flight.

        Reached from a signal handler, the standard-input watcher and the
        executor's report of a controller-initiated close, so it is idempotent
        — which `Event.set` already is.
        """
        self._stopped.set()

    def drain(self) -> None:
        """Stop fetching, drain what is in flight, and release the core."""
        if self._core is None:
            return
        _log.info("draining")
        self._core.shutdown()
        undrained = self._executor.stop() if self._executor else 0
        for undo in self._teardown:
            undo()
        self._teardown.clear()
        # The context is released only when nothing is still running against
        # it. A worker the bounded drain gave up on is inside user code and may
        # yet call the core; freeing the context under it would be a
        # use-after-free in native code. The process is on its way out either
        # way, so the kernel reclaims what this skips.
        if undrained == 0:
            self._core.free()
        self._core = None
        self._executor = None
        self._started = False
        self._release_slot()

    def __enter__(self) -> Self:
        """Start, unless the caller already did.

        A runner started explicitly — which is how a host injects a core, or
        sequences its own boot — still drains on the way out of a ``with``.
        """
        return self if self._started else self.start()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.stop()
        self.drain()

    # -- internals ---------------------------------------------------------

    def _release_slot(self) -> None:
        if self._holds_slot:
            self._holds_slot = False
            _started_slot.release()

    def _define(self, operator: Operator) -> Runner:
        if any(o.factory_id == operator.factory_id for o in self._operators):
            raise FlezhaError("topology.duplicate_factory_id", {"factory_id": operator.factory_id})
        self._operators.append(operator)
        if self._started:
            # Declaring on a live core appends to the set its connect frame is
            # serialized from, which is the resync of record. An FFI host ships
            # no `UpdateFactories` frame of its own: the ABI has no entry point
            # for one, so the controller learns the new type at the next bind.
            # The in-process Rust host, which owns the runner channel directly,
            # is the only one that can notify a live controller — OWED to
            # `sdk-abi-factory-resync`, and Java carries the same gap.
            self._declare(operator)
        return self

    def _declare(self, operator: Operator) -> None:
        assert self._core is not None
        graph.declare(operator.spec, self._core, self._registry)

    def _observe_shutdown(self) -> None:
        """Shutdown is observed three ways, because the harness, a user and
        the controller each have their own: end of file on standard input, a
        signal, and the core's own stop — which every worker sees as
        ``shutting_down`` from its next poll.

        The first two are the core's, on a thread of its own: it installs the
        SIGINT and SIGTERM handler when the runner starts, and the watch below
        is the same exit by another name. Nothing here reads a signal or a
        descriptor, so neither is missed while a handler body holds this
        interpreter.
        """
        if not sys.stdin.isatty():
            # The e2e harness manages a runner as an Erlang Port and stops it
            # by closing the port, which is this process's standard input. The
            # watch is the core's, on a thread that holds nothing the
            # interpreter's finalisation waits on.
            assert self._core is not None
            self._core.watch_stdin_eof(_STDIN_EOF_GRACE_MS)


def create_runner(namespace: str) -> Runner:
    """Start here: ``flezha.create_runner("default").register_factory(op).run()``.

    Creating a runner connects nothing, claims no slot and spawns no thread.
    """
    return Runner(namespace)
