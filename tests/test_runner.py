"""The runner's lifecycle: what it claims, when, and what it refuses."""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import subprocess
import sys
import threading
from typing import Any

import pytest

import flezha
from conftest import FakeCore, answered, demo, leave, work_activation
from flezha import logger, native
from flezha.errors import FlezhaError
from flezha.runner import Runner


@flezha.tasklet(name="other")
class Other:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)


@flezha.relay("other")
def other_op(c: flezha.Channels) -> None:
    c.input("in").into(Other).output()


def declared(r: Runner) -> list[str]:
    """The factory ids ``r`` declares."""
    return [o.factory_id for o in r._relays]  # type: ignore[attr-defined]


@pytest.fixture
def runner() -> Any:
    made: list[Runner] = []

    def build(namespace: str = "test") -> Runner:
        r = flezha.create_runner(namespace)
        made.append(r)
        return r

    yield build
    for r in made:
        try:
            r.close()
        except Exception:  # noqa: BLE001, S110
            # Teardown, after the assertions have run: a failure here must not
            # mask the one the test already made, and nothing it could raise is
            # actionable once the runner is on its way out.
            pass


def test_creating_a_runner_connects_nothing(runner: Any) -> None:
    r = runner("my_app")
    assert r.namespace == "my_app"
    with pytest.raises(FlezhaError) as refused:
        r.relays()
    assert refused.value.code == "runtime.runner_not_started"
    assert declared(r) == []


def test_start_declares_then_connects(runner: Any) -> None:
    core = FakeCore()
    r = runner("my_app").register_factory(demo).start(core=core)
    try:
        assert core.started == "my_app"
        assert len(core.declared) == 1
        assert json.loads(core.declared[0])["factory_id"] == "demo"
        assert declared(r) == ["demo"]
    finally:
        r.close()


def test_run_is_start_plus_await_termination(runner: Any) -> None:
    core = FakeCore()
    r = runner()
    r.register_factory(demo)

    def stop_soon() -> None:
        while core.started is None:
            threading.Event().wait(0.01)
        r.close()

    threading.Thread(target=stop_soon, daemon=True).start()
    r.run(core=core)
    assert core.shut_down


def test_zero_declared_types_is_legal(runner: Any) -> None:
    core = FakeCore()
    r = runner().start(core=core)
    try:
        assert core.declared == []
        assert core.started == "test"
    finally:
        r.close()


def test_a_second_start_on_the_same_runner_is_refused(runner: Any) -> None:
    core = FakeCore()
    r = runner().register_factory(demo).start(core=core)
    try:
        with pytest.raises(FlezhaError, match="runtime.runner_already_running"):
            r.start(core=FakeCore())
    finally:
        r.close()


def test_only_one_runner_may_be_started_per_process(runner: Any) -> None:
    first = runner().register_factory(demo).start(core=FakeCore())
    try:
        second = runner()
        with pytest.raises(FlezhaError, match="runtime.runner_already_running"):
            second.start(core=FakeCore())
    finally:
        first.close()
    # The slot is released by the drain, so the next runner may take it.
    third = runner().start(core=FakeCore())
    third.close()


def test_a_declaration_that_fails_releases_the_slot(runner: Any) -> None:
    @flezha.relay("broken")
    def broken(c: flezha.Channels) -> None:
        c.input("dangling")

    with pytest.raises(FlezhaError):
        runner().register_factory(broken).start(core=FakeCore())
    later = runner().start(core=FakeCore())
    later.close()


def test_a_post_start_define_reaches_the_core(runner: Any) -> None:
    core = FakeCore()
    r = runner().register_factory(demo).start(core=core)
    try:
        r.register_factory(other_op)
        assert [json.loads(body)["factory_id"] for body in core.declared] == ["demo", "other"]
        assert declared(r) == ["demo", "other"]
    finally:
        r.close()


def test_removing_an_undeclared_type_is_refused_naming_it(runner: Any) -> None:
    r = runner().register_factory(demo)
    with pytest.raises(FlezhaError) as caught:
        r.unregister_factory(other_op)
    assert (caught.value.code, caught.value.details) == (
        "runtime.class_not_registered",
        {"factory": "other"},
    )


def test_a_cell_that_left_starts_again_from_its_initial_state(runner: Any) -> None:
    from flezha.dispatch import dispatch_now

    core = FakeCore()
    r = runner().register_factory(demo).start(core=core)
    try:
        deps = r._executor._deps  # type: ignore[attr-defined]
        read = {**leave(3, whole_relay=False, tasklet=1), "kind": "live_state_read"}
        dispatch_now(work_activation(1, "abc", tasklet=1), core, deps)
        assert json.loads(answered(read, deps)["json"])["count"] == 3

        dispatch_now(leave(2, whole_relay=False, tasklet=1), core, deps)

        assert core.completed[-1] == {"token": 2, "outcome": "state_applied"}
        assert json.loads(answered(read, deps)["json"])["count"] == 0
    finally:
        r.close()


def test_removing_a_type_with_no_instances_succeeds(runner: Any) -> None:
    core = FakeCore()
    r = runner().register_factory(demo).register_factory(other_op).start(core=core)
    try:
        r.unregister_factory(other_op)
        assert declared(r) == ["demo"]
        assert core.undeclared == ["other"]
    finally:
        r.close()


def test_a_duplicate_factory_id_is_refused(runner: Any) -> None:
    r = runner().register_factory(demo)
    with pytest.raises(FlezhaError) as caught:
        r.register_factory(demo)
    assert (caught.value.code, caught.value.details) == (
        "topology.duplicate_factory_id",
        {"factory_id": "demo"},
    )


def test_register_modules_imports_and_registers(runner: Any) -> None:
    core = FakeCore()
    r = runner().register_modules("declaring_module").start(core=core)
    try:
        assert declared(r) == ["from_module", "from_module_short_form"]
    finally:
        r.close()


def test_an_unimportable_module_raises_its_own_import_error(runner: Any) -> None:
    with pytest.raises(ImportError) as caught:
        runner().register_modules("no_such_module_anywhere")
    assert "no_such_module_anywhere" in str(caught.value)


def test_the_context_manager_drains_on_exit(runner: Any) -> None:
    core = FakeCore()
    with runner().register_factory(demo).start(core=core) as r:
        assert declared(r) == ["demo"]
    assert core.shut_down
    assert core.freed


def test_the_startup_line_names_the_tier_the_executor_and_the_sizing(
    runner: Any, caplog: pytest.LogCaptureFixture
) -> None:
    core = FakeCore()
    with caplog.at_level(logging.INFO, logger="flezha.runner"):
        r = runner().register_factory(demo).start(core=core)
    try:
        line = caplog.text
        assert "tier" in line
        assert "executor" in line
        assert "base" in line and "ceiling" in line
    finally:
        r.close()


def test_a_failed_start_writes_what_the_core_said_while_connecting(
    runner: Any, caplog: pytest.LogCaptureFixture
) -> None:
    class Unreachable(FakeCore):
        def start(self, namespace: str) -> None:
            raise RuntimeError("the start wait ended")

        def take_notices(self) -> list[tuple[str, str, str]]:
            return [("warn", "flz_core::control_unit", "Connection attempt 1 failed: no socket")]

    with caplog.at_level(logging.WARNING, logger="flezha.core"), pytest.raises(RuntimeError):
        runner().register_factory(demo).start(core=Unreachable())
    assert "Connection attempt 1 failed: no socket" in caplog.text


def test_the_executor_is_an_axis_of_the_run(monkeypatch: pytest.MonkeyPatch, runner: Any) -> None:
    monkeypatch.setenv("FLEZHA_PYTHON_EXECUTOR", "asyncio")
    core = FakeCore()
    r = runner().register_factory(demo).start(core=core)
    try:
        assert r._executor.name == "asyncio"  # type: ignore[attr-defined]
    finally:
        r.close()


def test_an_unknown_executor_is_refused(monkeypatch: pytest.MonkeyPatch, runner: Any) -> None:
    monkeypatch.setenv("FLEZHA_PYTHON_EXECUTOR", "goroutines")
    with pytest.raises(FlezhaError) as caught:
        runner().register_factory(demo).start(core=FakeCore())
    assert (caught.value.code, caught.value.details["value"]) == (
        "usage.unknown_word",
        "goroutines",
    )


def test_a_controller_initiated_shutdown_ends_the_runners_own_wait(runner: Any) -> None:
    """Shutdown is observed three ways; this is the third.

    A worker that sees `shutting_down` from its next poll ends — but a runner
    that only watched its own stop would keep waiting for a termination nothing
    was going to signal.
    """
    core = FakeCore()
    r = runner().register_factory(demo)

    def close_from_the_controller() -> None:
        while core.started is None:
            threading.Event().wait(0.01)
        core.shut_down = True

    threading.Thread(target=close_from_the_controller, daemon=True).start()
    r.run(core=core)
    assert core.freed


def test_the_drain_frees_the_context(runner: Any) -> None:
    core = FakeCore()
    r = runner().register_factory(demo).start(core=core)
    r.close()
    assert core.shut_down
    assert core.freed


def test_a_boot_with_no_terminal_asks_the_core_to_watch_for_end_of_file(
    monkeypatch: pytest.MonkeyPatch, runner: Any
) -> None:
    """The harness stops a runner by closing its standard input; a runner that
    stopped asking would be one nothing can stop."""
    monkeypatch.setattr(sys, "stdin", io.StringIO())
    core = FakeCore()
    runner().register_factory(demo).start(core=core)
    assert core.stdin_watched


def test_a_start_takes_the_factory_first_and_answers_the_id_and_the_outcome(
    runner: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = FakeCore()
    calls: list[tuple[str, Any]] = []
    answers = iter(
        [
            {"id": "echo-k7m2qzxw4pd3fn2a", "outcome": "started"},
            {"id": "my_echo", "outcome": "already_running"},
        ]
    )

    def admin_call(verb: str, args: str) -> str:
        calls.append((verb, json.loads(args)))
        return json.dumps(next(answers))

    monkeypatch.setattr(core, "admin_call", admin_call)
    control = runner().start(core=core).control()
    assert control.start_relay("echo") == flezha.StartResult("echo-k7m2qzxw4pd3fn2a", "started")
    assert control.start_relay("echo", id="my_echo", init_values={"a": 1}) == flezha.StartResult(
        "my_echo", "already_running"
    )
    assert calls == [
        ("Relay.start", [{"factory_id": "echo"}]),
        ("Relay.start", [{"factory_id": "echo", "id": "my_echo", "init_values": {"a": 1}}]),
    ]


def test_a_plugin_is_enabled_by_name_and_bound_and_unbound_by_relay(
    runner: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = FakeCore()
    calls: list[tuple[str, Any]] = []

    def admin_call(verb: str, args: str) -> str:
        calls.append((verb, json.loads(args)))
        return "null"

    monkeypatch.setattr(core, "admin_call", admin_call)
    control = runner().start(core=core).control()
    control.enable_plugin("writer", "relay_file_output", opts={"path": "out.txt"})
    control.enable_plugin(
        "feed", "relay_websocket", single=True, relay_id="my_echo", hooks=[["on_output", {}]]
    )
    control.bind_plugin("writer", "my_echo")
    control.bind_plugin("writer", "my_other", hooks=["on_output"], channel="in")
    control.unbind_plugin("writer", "my_echo")
    control.disable_plugin("writer")
    control.list_plugins()
    assert calls == [
        (
            "PluginManager.enable",
            ["writer", {"provider": "relay_file_output", "opts": {"path": "out.txt"}}],
        ),
        (
            "PluginManager.enable",
            [
                "feed",
                {
                    "provider": "relay_websocket",
                    "opts": {},
                    "single": True,
                    "relay_id": "my_echo",
                    "hooks": [["on_output", {}]],
                },
            ],
        ),
        ("PluginManager.bind", ["writer", {"relay_id": "my_echo"}]),
        (
            "PluginManager.bind",
            ["writer", {"relay_id": "my_other", "hooks": ["on_output"], "channel": "in"}],
        ),
        ("PluginManager.unbind", ["writer", {"relay_id": "my_echo"}]),
        ("PluginManager.disable", ["writer", {}]),
        ("PluginManager.enabled", []),
    ]


def test_control_and_send_before_start_are_refused(runner: Any) -> None:
    with pytest.raises(FlezhaError, match="runtime.runner_not_started"):
        runner().control()
    with pytest.raises(FlezhaError, match="runtime.runner_not_started"):
        runner().send("my_echo", "x", "in")


def test_a_send_hands_its_options_to_the_core_and_reads_the_answer(
    runner: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = FakeCore()
    calls: list[tuple[Any, ...]] = []

    def send_message(*args: Any) -> str:
        calls.append(args)
        return "null" if args[-1] else json.dumps({"to": "prices#p1", "verdict": "absorbed"})

    monkeypatch.setattr(core, "send_message", send_message, raising=False)
    r = runner().start(core=core)
    r.send("my_echo", "hello", "in")
    verdict = r.send("prices", "x", "in", msg_id="fx/0/7", key="EURUSD")
    assert r.send("orders", "hé", "in", local=True) is None
    assert calls == [
        ("my_echo", "in", b"hello", None, None, False),
        ("prices", "in", b"x", "fx/0/7", "EURUSD", False),
        ("orders", "in", "hé".encode(), None, None, True),
    ]
    assert verdict == {"to": "prices#p1", "verdict": "absorbed"}


def test_a_call_sends_the_value_as_an_output_encodes_it_and_reads_the_answer_by_its_format(
    runner: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    core = FakeCore()
    calls: list[tuple[str, Any]] = []
    answers = iter(
        [
            {"answer": {"pair": "EUR/USD", "status": "placed"}, "answer_codec": "json"},
            {"answer": "done", "answer_codec": "text"},
            {"answer": base64.b64encode(b"\x01\x02").decode(), "answer_codec": "bytes"},
            {"accepted": True},
        ]
    )

    def admin_call(verb: str, args: str) -> str:
        calls.append((verb, json.loads(args)))
        return json.dumps(next(answers))

    monkeypatch.setattr(core, "admin_call", admin_call)
    r = runner().start(core=core)

    assert r.call("orders", "place", {"pair": "EUR/USD"}) == {"pair": "EUR/USD", "status": "placed"}
    assert r.call("orders", "audit", "checked") == "done"
    assert r.call("orders", "raw", b"\x09") == b"\x01\x02"
    assert r.call("orders", "audit", "checked") == {"accepted": True}

    def sent(payload: bytes) -> list[Any]:
        return [base64.b64encode(payload).decode(), {"payload": "base64"}]

    assert calls == [
        ("Relay.call_channel", ["orders", "place", *sent(b'{"pair":"EUR/USD"}')]),
        ("Relay.call_channel", ["orders", "audit", *sent(b"checked")]),
        ("Relay.call_channel", ["orders", "raw", *sent(b"\x09")]),
        ("Relay.call_channel", ["orders", "audit", *sent(b"checked")]),
    ]


def test_a_call_before_start_is_refused(runner: Any) -> None:
    with pytest.raises(FlezhaError, match="runtime.runner_not_started"):
        runner().call("orders", "place", {})


def test_relays_are_the_instances_the_core_reports_bound(runner: Any) -> None:
    core = FakeCore()
    core.bound = ["orders", "prices#p1"]
    r = runner().register_factory(demo).start(core=core)
    assert r.relays() == ["orders", "prices#p1"]


def test_the_double_answers_every_call_a_host_can_make_on_a_core() -> None:
    """``native.Core.ENTRY_POINTS`` is asserted against the extension in
    ``test_native.py``; nothing held it against the double, which is hand written.

    The name check is against the same set ``conftest`` generates, so it holds
    the list closed and proves no behaviour; the second half pins what a
    generated default does. What neither can see: a scripted method whose
    parameters or answer have drifted from the core's. That is the job of the
    test that drives the call.
    """
    reachable = {*native.Core.ENTRY_POINTS, "free"}
    missing = sorted(reachable - set(dir(FakeCore)))
    assert missing == [], "the double is missing calls a runner can make on its core"
    # An unscripted one answers `None`, and refuses a call the wrapper would.
    last_error = vars(FakeCore)["last_error"]
    assert last_error(FakeCore()) is None
    with pytest.raises(TypeError):
        last_error(FakeCore(), "an argument the wrapper does not take")


def test_importing_the_library_installs_no_handler() -> None:
    script = "import logging, flezha\nprint(logging.getLogger('flezha').handlers)"
    out = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == "[<NullHandler (NOTSET)>]"


def _a_core_then_two_lines(
    setup: str, env: dict[str, str], after: str = ""
) -> subprocess.CompletedProcess[str]:
    """A fresh interpreter that sets up its logging, creates a core, runs
    ``after``, and logs a warning and an info line on the SDK's logger."""
    script = (
        "import logging\n" + setup + "\n"
        "from flezha.native import Core\n"
        "Core.create().free()\n" + after + "\n"
        "logging.getLogger('flezha.test').warning('a warning')\n"
        "logging.getLogger('flezha.test').info('an info line')\n"
    )
    environ = {k: v for k, v in os.environ.items() if k != "FLEZHA_LOG_LEVEL"} | env
    return subprocess.run(
        [sys.executable, "-c", script], capture_output=True, text=True, env=environ, check=True
    )


def test_a_runner_starts_at_warn_on_standard_error() -> None:
    out = _a_core_then_two_lines("", {})
    assert out.stderr.splitlines() == ["[flezha] a warning"]


def test_flezha_log_level_sets_the_default_level() -> None:
    out = _a_core_then_two_lines("", {"FLEZHA_LOG_LEVEL": "info"})
    assert out.stderr.splitlines() == ["[flezha] a warning", "[flezha] an info line"]


def test_the_applications_logging_wins() -> None:
    setup = "import sys\nlogging.basicConfig(stream=sys.stdout, format='app %(message)s')"
    out = _a_core_then_two_lines(setup, {})
    assert (out.stdout.splitlines(), out.stderr) == (["app a warning"], "")


def test_the_applications_logging_wins_when_configured_after_the_core() -> None:
    setup = "import sys\nlogging.basicConfig(stream=sys.stdout, format='app %(message)s')"
    out = _a_core_then_two_lines("", {}, after=setup)
    assert (out.stdout.splitlines(), out.stderr) == (["app a warning"], "")


def test_one_call_turns_diagnostics_on() -> None:
    root = logger.configure("info")
    try:
        assert root.level == logging.INFO
        assert any(not isinstance(h, logging.NullHandler) for h in root.handlers)
    finally:
        for handler in list(root.handlers):
            if not isinstance(handler, logging.NullHandler):
                root.removeHandler(handler)
        root.setLevel(logging.NOTSET)


@flezha.relay(factory_id="short_flow", output="out", codec="text")
def short_flow(value: str) -> str:
    return value


@flezha.relay(factory_id="short_steps", run="steps", output="placed", codec="text")
def short_steps(value: str) -> str:
    return value


def test_register_factory_takes_a_short_form_of_either_kind(runner: Any) -> None:
    r = runner().register_factory(short_flow).register_factory(short_steps)
    assert declared(r) == ["short_flow", "short_steps"]

    r.unregister_factory(short_steps)
    assert declared(r) == ["short_flow"]
    r.unregister_factory(short_flow)
    assert declared(r) == []


def test_register_modules_collects_a_short_form(runner: Any) -> None:
    core = FakeCore()
    r = runner().register_modules("declaring_module").start(core=core)
    try:
        assert "from_module_short_form" in declared(r)
    finally:
        r.close()


def test_a_short_form_collides_with_one_of_the_other_kind(runner: Any) -> None:
    @flezha.relay(factory_id="short_flow", run="steps", output="placed")
    def twin(value: str) -> str:
        return value

    r = runner().register_factory(short_flow)
    with pytest.raises(FlezhaError) as caught:
        r.register_factory(twin)
    assert (caught.value.code, caught.value.details) == (
        "topology.duplicate_factory_id",
        {"factory_id": "short_flow"},
    )
    assert "short_flow" in str(caught.value)


def test_registering_a_tasklet_is_refused(runner: Any) -> None:
    with pytest.raises(FlezhaError) as caught:
        runner().register_factory(Other)  # type: ignore[arg-type]
    assert caught.value.code == "topology.no_relay_exported"
