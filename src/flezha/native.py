"""The compiled veneer over ``flz_core``'s C ABI, and the host rules around it.

:class:`flz_core_ffi.Core` itself is defined by the extension
(``python_glue.rs``) and is an opaque object rather than a process-wide handle —
which is what keeps a sub-interpreter executor a later swap. This module adds no
ABI surface: every method below forwards to exactly one veneer call through
:meth:`Core._call`, which is the **one** site that touches the native object.
What it adds is the fork guard, which is a host rule and not a core one, and the
translation of the extension's error documents into :class:`FlezhaError`.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from .errors import FlezhaError

if TYPE_CHECKING:
    from . import flz_core_ffi

__all__ = ["Core", "ACTIVATION_SCHEMA_VERSION", "load_extension", "build_tier"]

#: The activation schema version this package was written against. The
#: extension refuses a mismatch at context creation, naming both versions,
#: before any controller connection is attempted.
ACTIVATION_SCHEMA_VERSION = 3


def load_extension() -> Any:
    """Import the compiled veneer, or say what to do about its absence.

    An absent extension is an installation defect rather than a refusal, and
    nothing is left to render a catalogue message with, so it stays an
    :class:`ImportError`.
    """
    try:
        from . import flz_core_ffi as module
    except ImportError as error:  # pragma: no cover - exercised by a broken install
        raise ImportError(
            f"the flezha native extension could not be loaded ({error}). Install a prebuilt "
            f"wheel, or build from a checkout of the flezha repository, where "
            f"scripts/build-python-sdk.sh produces it."
        ) from error
    return module


def build_tier() -> str:
    """Which wheel tier is running: the GIL build or the free-threaded one.

    Reported in the runner's startup line, so a benchmark or a bug report can
    never be ambiguous about the conditions it was produced under.
    """
    return "free-threaded" if load_extension().free_threaded() else "gil"


class Core:
    """The process's core context, with the fork rule enforced around it.

    The core owns native threads — transport, flusher, the runner loop — and
    ``fork(2)`` carries none of them, so a child that inherited a started core
    would connect to nothing and serve nothing, silently.

    Every method below is one line through :meth:`_call`, which is the only
    place in this package that reaches the native object: the fork guard and the
    error translation therefore apply to all of them by construction, and
    ``test_native.py`` asserts there is no second site.
    """

    #: Every ABI entry point a running host reaches, in the veneer's own
    #: spelling. Read by ``test_native.py`` against what the veneer exports, so
    #: an entry point added there and not here is a red build rather than a
    #: method that quietly does not exist.
    ENTRY_POINTS = (
        "declare_class",
        "check_class",
        "start",
        "local_start",
        "local_on_sink",
        "local_feed",
        "local_next_output",
        "local_run_until_idle",
        "local_stop",
        "poll",
        "wait_starvation",
        "complete",
        "abandon",
        "shutdown",
        "watch_stdin_eof",
        "spawn_request",
        "spawn_result",
        "part",
        "effect_begin",
        "effect_end",
        "effect_release",
        "admin_call",
        "send_message",
        "relays",
        "last_error",
    )

    __slots__ = ("_native", "_owner_pid", "_module")

    def __init__(self, native: flz_core_ffi.Core, module: Any) -> None:
        self._native = native
        self._module = module
        # Recorded at construction rather than at `start`, so a fork between
        # the two is caught by the same guard.
        self._owner_pid = os.getpid()

    @classmethod
    def create(cls) -> Core:
        """Create the context, negotiating the activation schema version."""
        module = load_extension()
        try:
            native = module.Core(ACTIVATION_SCHEMA_VERSION)
        except module.NativeError as error:
            raise FlezhaError.from_document(str(error)) from error
        return cls(native, module)

    # -- the one crossing --------------------------------------------------

    def _call(self, entry: str, *args: Any) -> Any:
        """Guard, cross, and translate. The only site that touches the veneer."""
        if os.getpid() != self._owner_pid:
            raise FlezhaError(
                "runtime.forked_process", {"owner": self._owner_pid, "pid": os.getpid()}
            )
        try:
            return getattr(self._native, entry)(*args)
        except self._module.NativeError as error:
            # The extension's message is the core's JSON error document.
            raise FlezhaError.from_document(str(error)) from error

    # -- lifecycle ---------------------------------------------------------

    def free(self) -> None:
        """Release what the context creation made."""
        self._call("free")

    def declare_class(self, decl_json: bytes) -> None:
        """Declare one host class from its JSON declaration."""
        self._call("declare_class", decl_json)

    def check_class(self, decl_json: bytes) -> bytes:
        """Judge one declaration without registering it; answers the derived one."""
        answer: bytes = self._call("check_class", decl_json)
        return answer

    def start(self, namespace: str) -> None:
        """Connect to the controller and start the runner loop."""
        self._call("start", namespace)

    # -- local mode: the declared graph here, with no controller -----------

    def local_start(self, factory_id: str, relay_id: str) -> None:
        """Start the declared class as a local run. The alternative to
        :meth:`start`, never an addition."""
        self._call("local_start", factory_id, relay_id)

    def local_on_sink(self, channel: str) -> None:
        """Queue a declared output for :meth:`local_next_output`."""
        self._call("local_on_sink", channel)

    def local_feed(self, channel: str, payload: bytes) -> None:
        """Feed one value's wire bytes to a declared entry channel."""
        self._call("local_feed", channel, payload)

    def local_next_output(self, timeout_ms: int) -> Any:
        """The next value an output released, or ``None`` on timeout."""
        return self._call("local_next_output", timeout_ms)

    def local_run_until_idle(self, timeout_ms: int) -> None:
        """Park until the graph is idle; a timeout is a refusal."""
        self._call("local_run_until_idle", timeout_ms)

    def local_stop(self) -> None:
        """End the local run and release its Cells."""
        self._call("local_stop")

    def shutdown(self) -> None:
        """Ship every Cell's state to the controller, then wake every blocked
        poller and stop the runner loop."""
        self._call("shutdown")

    def watch_stdin_eof(self, grace_ms: int) -> None:
        """Ask the core to watch standard input and end this process on end of
        file, on a thread of its own — so no reader of descriptor 0 lives here
        and none holds a lock the interpreter's finalisation waits on."""
        self._call("watch_stdin_eof", grace_ms)

    # -- the poll loop -----------------------------------------------------

    def poll(self, timeout_ms: int) -> Any:
        """One activation, ``None`` on timeout, or ``"shutting_down"``."""
        return self._call("poll", timeout_ms)

    def wait_starvation(self, timeout_ms: int) -> int:
        """The ABI's three-way starvation status: 1 observed, 0 timeout, 2 closing."""
        return self._call("wait_starvation", timeout_ms)

    def complete(self, result: dict[str, Any]) -> None:
        """Answer one activation."""
        self._call("complete", result)

    def abandon(self, token: int) -> None:
        """Re-hand a polled activation's untouched items and release its cell."""
        self._call("abandon", token)

    def spawn_request(
        self,
        token: int,
        factory_id: str,
        id: str,
        payload: bytes,
        if_running: str,
        replay: bool,
        offset: int | None,
    ) -> int:
        """Request a spawn from the activation ``token``; answers its handle at once.
        ``offset`` is the input-log offset of the item the request runs under, if any."""
        return int(
            self._call("spawn_request", token, factory_id, id, payload, if_running, replay, offset)
        )

    def spawn_result(self, handle: int, timeout_ms: int) -> str | None:
        """A handle's answer as JSON, or ``None`` while it is pending."""
        answer = self._call("spawn_result", handle, timeout_ms)
        return None if answer is None else str(answer)

    def part(self, token: int, item: int, payload: bytes) -> None:
        """Send a part of work item ``item`` of the activation ``token``. Never waits."""
        self._call("part", token, item, payload)

    def effect_begin(self, token: int, item: int, name: str) -> tuple[str, bytes | None]:
        """Begin ``ctx.effect(name)`` in work item ``item``: ``("kept", result)``,
        ``("run", None)`` or ``("unrecorded", None)``. Never waits."""
        status, kept = self._call("effect_begin", token, item, name)
        return str(status), kept

    def effect_end(self, token: int, item: int, payload: bytes) -> bytes | None:
        """Keep a ``run`` begin's encoded result; waits for the controller.
        ``None`` keeps this result; otherwise the result kept first."""
        return self._call("effect_end", token, item, payload)

    def effect_release(self, token: int, item: int) -> None:
        """End a begun effect without keeping anything. Never waits."""
        self._call("effect_release", token, item)

    def admin_call(self, verb: str, args: str) -> str:
        return str(self._call("admin_call", verb, args))

    def send_message(
        self,
        relay: str,
        channel: str,
        message: bytes,
        msg_id: str | None,
        key: str | None,
        local: bool,
    ) -> str:
        """Send one message, through the controller or locally, answering as
        JSON: the verdict, or ``null`` for a local send."""
        return str(self._call("send_message", relay, channel, message, msg_id, key, local))

    def relays(self) -> list[str]:
        """The ids of the relay instances bound on this runner."""
        return list(self._call("relays"))

    # -- diagnostics -------------------------------------------------------

    def last_error(self) -> str | None:
        """The context's last recorded error, or ``None``."""
        answer = self._call("last_error")
        return None if answer is None else str(answer)

    @property
    def native(self) -> Any:
        """The veneer object itself, for a test that drives it directly."""
        return self._native

    # -- context-free entry points ------------------------------------------
    #
    # These take no context, so they take no guard: they read a process-global
    # clock, which a fork does not break.

    def monotonic_nanos(self) -> int:
        """The core's monotonic clock in nanoseconds."""
        return int(self._module.monotonic_nanos())
