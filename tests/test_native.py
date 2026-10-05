"""The veneer as this package uses it: its surface, its guards and its views.

The mapping itself is proved on the Rust side, by the ABI scenario suite under
``--features python``, which drives every activation kind through the veneer
with no Python process involved. What is proved here is what only Python can
see: the surface this package wraps, the fork rule, and the payload view's
contract.
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from flezha import native
from flezha.errors import FlezhaError

ffi = native.load_extension()


def test_the_extension_loads_and_speaks_this_schema() -> None:
    assert ffi.schema_version() == native.ACTIVATION_SCHEMA_VERSION


def test_the_build_tier_is_reported() -> None:
    assert native.build_tier() in ("gil", "free-threaded")
    assert isinstance(ffi.free_threaded(), bool)


def test_every_entry_point_this_package_calls_is_one_the_veneer_exports() -> None:
    """The wrapper adds no ABI surface.

    A veneer entry point added there and not here would be a method that
    quietly does not exist; one added here and not there would raise at the
    first call rather than at import.
    """
    exported = {name for name, _abi, _blocking in ffi.native_surface()}
    for entry in native.Core.ENTRY_POINTS:
        assert f"Core.{entry}" in exported or entry in exported, entry
    assert hasattr(ffi.Core, "free")


def test_every_crossing_names_a_declared_entry_point() -> None:
    """`_call` is the only site that touches the veneer, and it names by string.

    `ENTRY_POINTS` is what the test above checks against the extension, so a
    method forwarding to a name that is not in it would cross unchecked.
    """
    import ast
    import pathlib

    source = pathlib.Path(native.__file__).read_text()
    crossings = {
        node.args[0].value
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_call"
        and node.args
        and isinstance(node.args[0], ast.Constant)
    }
    assert crossings, "no crossing found; `_call` is how this module reaches the veneer"
    unknown = crossings - set(native.Core.ENTRY_POINTS) - {"free"}
    assert not unknown, f"{sorted(unknown)} is forwarded but not declared in ENTRY_POINTS"


def test_every_export_names_a_c_abi_entry_point() -> None:
    for name, abi, _blocking in ffi.native_surface():
        assert abi.startswith("flz_core_"), f"{name} names {abi}"


def test_a_schema_mismatch_refuses_before_connecting() -> None:
    with pytest.raises(ffi.SchemaMismatch) as caught:
        ffi.Core(native.ACTIVATION_SCHEMA_VERSION + 1)
    error = FlezhaError.from_document(str(caught.value))
    assert (error.code, error.details) == (
        "runtime.schema_mismatch",
        {"shim": native.ACTIVATION_SCHEMA_VERSION + 1, "core": native.ACTIVATION_SCHEMA_VERSION},
    )


def test_a_schema_mismatch_reaches_the_package_as_its_own_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(native, "ACTIVATION_SCHEMA_VERSION", ffi.schema_version() + 1)
    with pytest.raises(FlezhaError) as caught:
        native.Core.create()
    assert (caught.value.code, caught.value.class_) == ("runtime.schema_mismatch", "invalid")
    assert caught.value.details["core"] == ffi.schema_version()


def test_a_context_is_created_used_and_freed_through_the_object() -> None:
    core = native.Core.create()
    try:
        assert core.last_error() is None
        with pytest.raises(FlezhaError) as caught:
            core.declare_class(b'{"factory_id":"x"}')
        # The engine's own detail survives the translation into the one error.
        assert caught.value.code == "topology.declaration_unreadable"
        assert "sdk_lang" in caught.value.details["reason"]
        assert caught.value.__cause__ is not None
        assert core.last_error() is not None
    finally:
        core.free()


def _checkpoint_class(factory_id: str, ack: Any) -> dict[str, Any]:
    """A tier class under one checkpoint acknowledgement, as the core reads it."""
    import flezha
    from conftest import declare as submit
    from conftest import echo
    from flezha import graph

    @flezha.relay(factory_id, run={"flow": {"exactly": {"effectively_once": {"ack": ack}}}})
    def relay(c: flezha.Channels) -> None:
        c.input("input").into(echo, f"{factory_id}_tasklet").keyed().sink("sink")

    assert submit(relay)["ack_granularity"] == "checkpoint"
    return graph.to_host_class_decl(relay.spec)


def test_holding_outputs_at_the_checkpoint_registers_with_its_delay() -> None:
    ack = {"checkpoint": {"outputs": "at_checkpoint", "max_delay_ms": 1500}}
    decl = _checkpoint_class("outputs_only", ack)
    assert decl["run"]["flow"]["exactly"]["effectively_once"]["ack"] == ack

    core = native.Core.create()
    try:
        core.declare_class(json.dumps(decl).encode())
    finally:
        core.free()


def test_a_freed_context_refuses_rather_than_dereferencing() -> None:
    core = native.Core.create()
    core.free()
    core.free()
    with pytest.raises(FlezhaError) as caught:
        core.declare_class(b"{}")
    assert caught.value.code == "internal.uncatalogued"


def test_the_fork_guard_names_the_rule() -> None:
    """Every entry point checks, because there is no site left to forget it at."""
    core = native.Core.create()
    try:
        core._owner_pid = os.getpid() + 1
        with pytest.raises(FlezhaError) as caught:
            core.poll(0)
        assert (caught.value.code, caught.value.details) == (
            "runtime.forked_process",
            {"owner": os.getpid() + 1, "pid": os.getpid()},
        )
        # A second entry point, to prove the guard is generated rather than
        # written at one call site.
        with pytest.raises(FlezhaError, match="runtime.forked_process"):
            core.shutdown()
        core._owner_pid = os.getpid()
    finally:
        core.free()


def test_a_context_free_call_is_guarded_too() -> None:
    core = native.Core.create()
    core._owner_pid = os.getpid() + 1
    with pytest.raises(FlezhaError, match="runtime.forked_process"):
        core.free()
    core._owner_pid = os.getpid()
    core.free()


def test_the_context_is_callable_from_a_thread_that_did_not_create_it() -> None:
    """A worker pool is threads that did not create the context.

    The ABI accepts every entry point from any thread — `flz_core_complete`
    explicitly so — and the veneer must not narrow that. Marking the pyclass
    `unsendable` does exactly that, and the pool's own tests, which drive a
    Python fake, would never see it.
    """
    import threading

    core = native.Core.create()
    answers: list[object] = []
    errors: list[BaseException] = []

    def poll_from_another_thread() -> None:
        try:
            answers.append(core.poll(50))
            answers.append(core.wait_starvation(50))
        except BaseException as error:  # noqa: BLE001 - the panic is the defect
            errors.append(error)

    worker = threading.Thread(target=poll_from_another_thread)
    worker.start()
    worker.join(timeout=10)
    try:
        assert errors == [], errors
        assert answers == [None, 0]
    finally:
        core.free()


def test_a_spawn_with_no_control_plane_answers_unavailable_until_its_invocation_ends() -> None:
    """The two spawn entries against the real core, in a local run, where
    nothing listens on the control plane: every read inside the invocation
    answers the same, and a read after it raises released."""
    import flezha

    reads: list[tuple[str, str]] = []
    unread: list[flezha.SpawnHandle] = []

    @flezha.tasklet(name="spawner")
    class Spawner:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            handle = ctx.spawn_steps("worker", value, id="w1")
            for read in (handle.try_result, handle.wait):
                try:
                    read()
                except flezha.FlezhaError as error:
                    reads.append((error.code, error.class_))
            unread.append(ctx.spawn_steps("worker", value))

    @flezha.relay("spawn_without_control_plane")
    def relay(c: flezha.Channels) -> None:
        c.input("in", codec="text").into(Spawner).sink("out")

    with flezha.LocalRelay.start(relay) as run:
        run.feed("in", "p")
        run.run_until_idle(10_000)
        with pytest.raises(FlezhaError) as released:
            unread[0].wait()
    assert released.value.code == "runtime.spawn_handle_released"
    assert reads == [("spawn.unavailable", "unavailable")] * 2


def test_the_clock_needs_no_context() -> None:
    core = native.Core.create()
    try:
        assert core.monotonic_nanos() > 0
    finally:
        core.free()


def test_the_payload_view_is_a_buffer_and_copies_only_on_request() -> None:
    """The common decode path allocates no intermediate ``bytes``."""
    import msgpack

    body = msgpack.packb({"a": 1})
    view = _view_of(body)
    assert not isinstance(view, bytes)
    assert len(view) == len(body)
    assert msgpack.unpackb(view) == {"a": 1}
    assert bytes(view) == body
    assert view.tobytes() == body


def test_a_released_view_names_the_rule_rather_than_serving_stale_bytes() -> None:
    view = _view_of(b"\x01\x02")
    view.release()
    with pytest.raises(BufferError):
        memoryview(view)
    # The copy taken before the release is unaffected: it is the caller's.
    assert view.tobytes() == b"\x01\x02"


def test_the_dispatcher_releases_every_view_an_activation_carried() -> None:
    """The view's validity ends with the invocation, wherever it was nested."""
    from flezha.dispatch import _release_views

    activation = {
        "items": [{"payload": _view_of(b"a")}, {"payload": _view_of(b"b")}],
        "changes": [{"bytes": _view_of(b"c")}],
    }
    _release_views(activation)
    for value in (
        *[item["payload"] for item in activation["items"]],
        activation["changes"][0]["bytes"],
    ):
        with pytest.raises(BufferError):
            memoryview(value)


def _view_of(body: bytes) -> Any:
    """A payload view carrying ``body``."""
    return ffi.PayloadView(body)


def test_the_core_refuses_a_local_send_held_elsewhere_or_with_an_id() -> None:
    """Against the real core: no runner here holds ``elsewhere``, and only the
    controller reads an id."""
    core = native.Core.create()
    try:
        with pytest.raises(FlezhaError) as caught:
            core.send_message("elsewhere", "in", b"x", None, None, True)
        assert caught.value.code == "runtime.relay_not_local"
        with pytest.raises(FlezhaError) as caught:
            core.send_message("orders", "in", b"x", "m-1", None, True)
        assert caught.value.code == "usage.options_conflict"
        assert isinstance(core.relays(), list)
    finally:
        core.free()
