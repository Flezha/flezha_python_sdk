"""The tasklet test harness, at parity with the Rust and Java ones."""

from __future__ import annotations

import asyncio
import re
from typing import Any

import pytest

import flezha
from conftest import Bumped, Counter, compensating, count_up, echo
from flezha.builder import apply_transforms
from flezha.cell_state import fold_restore
from flezha.identity import stable_id
from flezha.testing import ReplayMismatch, TaskletTestHarness, replay_equals


def test_a_handler_is_testable_with_no_runtime_and_no_controller() -> None:
    """A tasklet class through the harness, a short form's function directly."""

    @flezha.relay(id="shout", output="out")
    def shout(value: str) -> str:
        return value.upper()

    outcomes = TaskletTestHarness(echo).apply_inputs(["plain", "again"])
    assert [outcome.outputs for outcome in outcomes] == [["plain"], ["again"]]
    assert shout.tasklet is not None
    assert shout.tasklet("plain") == "PLAIN"


def test_the_harness_reports_state_changes_and_emits() -> None:
    harness: TaskletTestHarness[Counter] = TaskletTestHarness(count_up)
    outcome = harness.feed("abcd")
    assert harness.state == Counter(count=4, seen=["4"])
    assert outcome.changes == [Bumped(by=4)]
    assert outcome.outputs == [{"total": 4}]


def test_apply_inputs_runs_them_in_order() -> None:
    harness: TaskletTestHarness[Counter] = TaskletTestHarness(count_up)
    harness.apply_inputs(["a", "bb", "ccc"])
    assert harness.state == Counter(count=6, seen=["1", "2", "3"])


def test_construction_from_an_initial_state() -> None:
    harness: TaskletTestHarness[Counter] = TaskletTestHarness(
        count_up, initial_state=Counter(count=10, seen=["seeded"])
    )
    harness.feed("a")
    assert harness.state.count == 11


def test_construction_from_a_restore_payload() -> None:
    source: TaskletTestHarness[Counter] = TaskletTestHarness(count_up)
    source.apply_inputs(["a", "bb"])
    snapshot = source.snapshot()

    restored: TaskletTestHarness[Counter] = TaskletTestHarness(count_up)
    assert restored.restore(snapshot=snapshot) == source.state


def test_the_replay_gate_passes_on_a_pure_fold() -> None:
    assert replay_equals(count_up, ["a", "bb", "ccc"])


def test_the_replay_gate_reports_both_states_on_a_mismatch() -> None:
    """An impure fold is lost on restart and reported nowhere until then."""
    drift = {"n": 0}

    class Impure:
        def run(self, value: str, ctx: flezha.Ctx[dict[str, int], None]) -> None:
            ctx.change({"by": 1})

        def init_state(self, key: str | None) -> dict[str, int]:
            return {"total": 0}

        def apply_change(self, state: dict[str, int], change: dict[str, int]) -> None:
            # Not a pure fold: the answer depends on how many times it has run.
            drift["n"] += 1
            state["total"] += change["by"] * drift["n"]

    task = flezha.tasklet(Impure, name="impure")
    harness: TaskletTestHarness[dict[str, int]] = TaskletTestHarness(task)
    harness.apply_inputs(["a", "b"])
    with pytest.raises(ReplayMismatch) as caught:
        harness.replay_equals()
    assert caught.value.live != caught.value.replayed


def test_the_replay_gate_catches_a_mutation_that_recorded_no_change() -> None:
    """The gate catches what the state guard cannot: an unguarded state shape."""

    class Sneaky:
        def run(self, value: str, ctx: flezha.Ctx[dict[str, int], None]) -> None:
            # A plain dict is not a declared state, so nothing refuses this —
            # the replay gate is what reports it.
            ctx.state["total"] = ctx.state["total"] + 1

        def init_state(self, key: str | None) -> dict[str, int]:
            return {"total": 0}

        def apply_change(self, state: dict[str, int], change: dict[str, int]) -> None:
            state["total"] += change["by"]

    task = flezha.tasklet(Sneaky, name="sneaky")
    harness: TaskletTestHarness[dict[str, int]] = TaskletTestHarness(task)
    harness.feed("a")
    with pytest.raises(ReplayMismatch):
        harness.replay_equals()


def test_timers_are_recorded_and_fired() -> None:
    fired: list[int] = []

    class Timed:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            ctx.timer_after(1000)

        def on_timer(self, timer: dict[str, object], ctx: flezha.Ctx[None, None]) -> None:
            fired.append(int(timer["scheduled_at"]))  # type: ignore[arg-type]

    task = flezha.tasklet(Timed, name="timed")
    harness: TaskletTestHarness[None] = TaskletTestHarness(task)
    outcome = harness.feed("x", now=500)
    assert outcome.timers == [{"kind": "processing_time", "scheduled_at": 1500, "register": True}]
    harness.fire_timer("processing_time", 1500)
    assert fired == [1500]


def test_event_time_is_advanced_through_the_registered_timers() -> None:
    fired: list[int] = []

    class EventTimed:
        def run(self, value: dict[str, int], ctx: flezha.Ctx[None, None]) -> None:
            ctx.timer_at_event_time(value["at"])

        def on_timer(self, timer: dict[str, object], ctx: flezha.Ctx[None, None]) -> None:
            fired.append(int(timer["scheduled_at"]))  # type: ignore[arg-type]

    task = flezha.tasklet(EventTimed, name="event_timed")
    harness: TaskletTestHarness[None] = TaskletTestHarness(task)
    harness.feed({"at": 300})
    harness.feed({"at": 100})
    harness.feed({"at": 900})
    harness.advance_event_time(500)
    assert fired == [100, 300]


def test_event_time_fires_each_pending_timer_once_and_never_a_cancelled_one() -> None:
    fired: list[int] = []

    class Cancelling:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            ctx.timer_at_event_time(100)
            ctx.timer_at_event_time(100)
            cancelled = ctx.timer_at_event_time(200)
            ctx.timer_at_event_time(300)
            ctx.cancel_timer("event_time", cancelled)

        def on_timer(self, timer: dict[str, object], ctx: flezha.Ctx[None, None]) -> None:
            fired.append(int(timer["scheduled_at"]))  # type: ignore[arg-type]

    harness: TaskletTestHarness[None] = TaskletTestHarness(
        flezha.tasklet(Cancelling, name="cancelling")
    )
    harness.feed("x")
    assert len(harness.advance_event_time(150)) == 1
    assert len(harness.advance_event_time(400)) == 1
    assert fired == [100, 300]


def test_a_compensation_is_driven_over_recorded_outputs() -> None:
    seen: list[Any] = []
    harness: TaskletTestHarness[None] = TaskletTestHarness(compensating(seen))
    harness.compensate(["a", "b"])
    assert seen == ["a", "b"]


def test_a_compensate_hook_that_returns_a_value_is_refused_as_the_runner_refuses_it() -> None:
    class Returning:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

        def compensate(self, outputs: list[Any], ctx: flezha.Ctx[None, str]):
            return "undo"

    harness: TaskletTestHarness[None] = TaskletTestHarness(
        flezha.tasklet(Returning, name="returning")
    )
    with pytest.raises(flezha.NonRetryableError) as caught:
        harness.compensate(["a"])
    assert str(caught.value) == (
        "returning: `compensate` returned a str; a value leaves a tasklet only through "
        "`ctx.emit`, so `compensate` returns None"
    )


def test_an_async_handler_needs_no_event_loop_in_the_test() -> None:
    @flezha.tasklet(name="slow")
    class Slow:
        async def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            await asyncio.sleep(0)
            ctx.emit(value.upper())

    harness: TaskletTestHarness[None] = TaskletTestHarness(Slow)
    assert harness.feed("hi").outputs == ["HI"]


def test_the_context_clock_and_identity_are_stable_within_an_invocation() -> None:
    ids: list[str] = []

    @flezha.tasklet(name="reads")
    class Reads:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            assert ctx.now() == ctx.now() == ctx.now()
            ids.append(ctx.stable_id())
            ids.append(ctx.stable_id())

    harness: TaskletTestHarness[None] = TaskletTestHarness(Reads, cell_key="e")
    harness.feed("x", now=99)
    assert ids[0] != ids[1]

    replayed: list[str] = []
    ids_backup = list(ids)
    ids.clear()
    again: TaskletTestHarness[None] = TaskletTestHarness(Reads, cell_key="e")
    again.feed("x", now=99)
    replayed = list(ids)
    # The n-th call of a replayed invocation equals the n-th of the original.
    assert replayed == ids_backup


@flezha.tasklet(name="misuses")
class Misuses:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        if value == "channel":
            ctx.emit(value, channel="elsewhere")
        elif value == "metadata":
            ctx.emit(value, metadata={"flz.cell": "forged"})
        else:
            ctx.change({"n": 1})


@pytest.mark.parametrize(
    ("misuse", "code", "details"),
    [
        ("channel", "runtime.channel_not_produced", {"tasklet": "misuses", "channel": "elsewhere"}),
        ("metadata", "runtime.reserved_metadata_key", {"key": "flz.cell"}),
        ("change", "runtime.change_without_apply_change", {"tasklet": "misuses"}),
    ],
)
def test_a_runtime_misuse_raises_the_one_error_with_its_code(
    misuse: str, code: str, details: dict[str, str]
) -> None:
    with pytest.raises(flezha.FlezhaError) as caught:
        TaskletTestHarness(Misuses).feed(misuse)
    assert (caught.value.code, caught.value.details) == (code, details)


def test_stable_id_matches_the_cross_language_vector() -> None:
    assert stable_id(b"instance/op/key", 42, 0) == "bbf81fd4955afd1724203700a240e822"
    assert stable_id(b"instance/op/key", 42, 1) == "bbf81fd4965afd1724203700a240e95d"
    assert stable_id(b"", 0, 0) == "15915375576d2f389a938dc3a9940c9d"


@pytest.mark.parametrize(
    ("payload", "code"),
    [
        ({"changes": [{"bytes": b""}]}, "runtime.change_without_apply_change"),
        ({"snapshot": {"from_version": 1, "bytes": b""}}, "runtime.hook_missing"),
    ],
)
def test_a_restore_the_tasklet_cannot_fold_names_the_hook(
    payload: dict[str, Any], code: str
) -> None:
    with pytest.raises(flezha.FlezhaError) as caught:
        fold_restore(echo, payload, None, "echo")
    assert caught.value.code == code


def test_a_missing_hook_is_refused_by_name() -> None:
    harness: TaskletTestHarness[Counter] = TaskletTestHarness(count_up)
    for fire, code in (
        (lambda: harness.fire_timer("processing_time", 1), "runtime.timer_without_on_timer"),
        (lambda: harness.compensate([]), "runtime.hook_missing"),
    ):
        with pytest.raises(flezha.FlezhaError) as caught:
            fire()
        assert caught.value.code == code


def test_the_harness_reports_a_string_and_a_dict_exactly_as_emitted() -> None:
    """The harness reads its sink back as MessagePack, so a string output is
    reported as the string rather than as the bytes a plugin would receive."""

    @flezha.tasklet(name="harness_emitter")
    class Emitter:
        def run(self, value: str, ctx: flezha.Ctx[None, Any]) -> None:
            ctx.emit({"said": value})
            ctx.emit(f"echo: {value}")

    assert TaskletTestHarness(Emitter).feed("hi").outputs == [{"said": "hi"}, "echo: hi"]


def test_the_router_delivers_each_document_once_on_each_keys_own_traffic() -> None:
    c = flezha.Channels("routed")
    config, trades = c.input("config"), c.input("trades")
    c.route_latest(config, trades, lambda trade: trade["symbol"])
    (router,) = c.tasklets
    assert router.name == "route_latest"
    harness: TaskletTestHarness[dict[str, Any]] = TaskletTestHarness(router.task)

    def fed(entry: int, value: dict[str, Any]) -> list[Any]:
        """Feed one value through the router's own source edge, as it is delivered."""
        (mapped,) = apply_transforms(c.channels[entry].chain_edges[0].transforms, value)
        return harness.feed(mapped).outputs

    document = {"limit": 5}
    assert fed(0, document) == []
    # The pair crosses the class codec, which answers a tuple as a list.
    assert fed(1, {"symbol": "A"}) == [["A", document]]
    assert fed(1, {"symbol": "B"}) == [["B", document]]
    assert fed(0, {"limit": 5}) == []
    assert fed(1, {"symbol": "A"}) == []
    assert harness.state == {"document": document, "version": 1, "delivered": {"A": 1, "B": 1}}

    changed = {"limit": 7}
    assert fed(0, changed) == []
    assert fed(1, {"symbol": "A"}) == [["A", changed]]
    assert harness.state["version"] == 2
    assert harness.replay_equals()


def test_the_harness_answers_a_spawn_already_resolved() -> None:
    results: list[flezha.SpawnResult | None] = []
    again: list[flezha.SpawnResult] = []
    unread: list[flezha.SpawnHandle] = []

    @flezha.tasklet(name="spawner")
    class Spawner:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            results.append(ctx.spawn_steps("worker", value).try_result())
            named = ctx.spawn_steps("worker", value, id="w1")
            results.append(named.try_result())
            again.append(named.wait())
            unread.append(ctx.spawn_steps("worker", value))

    harness: TaskletTestHarness[None] = TaskletTestHarness(Spawner)
    harness.feed("a")
    harness.feed("a", replay=True)
    generated, named, replayed, replayed_named = results
    assert generated is not None
    assert re.fullmatch(r"worker-[a-z2-7]{16}", generated.id)
    assert (generated.outcome, named) == ("spawned", flezha.SpawnResult("w1", "spawned"))
    assert (replayed, replayed_named) == (
        flezha.SpawnResult("", "skipped_on_replay"),
        flezha.SpawnResult("w1", "skipped_on_replay"),
    )
    assert again == [named, replayed_named]
    # As live: a handle is read inside the invocation that made it.
    with pytest.raises(flezha.FlezhaError, match="runtime.spawn_handle_released"):
        unread[0].wait()
