"""The one tasklet form: a named class whose value leaves only through `ctx`.

A function is a tasklet only as a short form's body, whose return value is its
output. Everything here is refused or answered before a controller is involved,
or by the dispatcher over a fake core.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

import flezha
from conftest import (
    FakeCore,
    answered,
    declare,
    deps_for,
    key_request,
    solo,
    solo_deps,
    wire_work,
    work_activation,
)
from flezha import graph
from flezha.dispatch import DispatchDeps
from flezha.errors import DeclarationError
from flezha.testing import TaskletTestHarness


def _effect(deps: DispatchDeps, value: Any) -> dict[str, Any]:
    (effect,) = answered(work_activation(1, value, factory="solo"), deps)["effects"]
    return effect


def _record(operator: flezha.Operator) -> dict[str, Any]:
    return graph.to_host_class_decl(operator.spec)["tasklets"][0]


# -- a class, named ------------------------------------------------------------


def test_a_function_tasklet_is_refused_pointing_at_the_short_form() -> None:
    def price(tick: str) -> str:
        return tick

    for declare_it in (flezha.tasklet(name="price"), lambda fn: flezha.tasklet(fn, name="price")):
        with pytest.raises(DeclarationError) as caught:
            declare_it(price)  # type: ignore[arg-type]
        message = str(caught.value)
        assert "price: is a function" in message
        assert "a tasklet is a class whose `run(self, value, ctx)`" in message
        assert "@flezha.unbounded_operator" in message
        assert "@flezha.bounded_operator" in message


class Upcase:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value.upper())


@pytest.mark.parametrize("declared", [{}, {"name": ""}])
def test_a_tasklet_without_a_name_is_refused_naming_the_class(declared: dict[str, Any]) -> None:
    with pytest.raises(DeclarationError) as caught:
        flezha.tasklet(Upcase, **declared)
    assert "Upcase: declares no `name`" in str(caught.value)


class Tally:
    def run(self, word: str, ctx: flezha.Ctx[dict[str, int], None]) -> None:
        ctx.change(word)

    def apply_change(self, state: dict[str, int], word: Any) -> None:
        state[word] = state.get(word, 0) + 1


class WordTally(Tally):
    """`Tally` renamed: the declared name, not the class, is where state lives."""


def test_a_renamed_class_keeps_its_name_and_its_state() -> None:
    before = flezha.tasklet(Tally, name="tally")
    after = flezha.tasklet(WordTally, name="tally")

    @flezha.unbounded_operator("tally_op")
    def renamed(c: flezha.Channels) -> None:
        c.input("words").into(after)

    assert (before.name, after.name, _record(renamed)["name"]) == ("tally", "tally", "tally")
    saved: TaskletTestHarness[dict[str, int]] = TaskletTestHarness(before)
    saved.apply_inputs(["a", "b", "a"])
    restarted: TaskletTestHarness[dict[str, int]] = TaskletTestHarness(after)
    assert restarted.restore(changes=saved.log) == {"a": 2, "b": 1}


# -- the value leaves through the context ----------------------------------------


def test_an_annotated_return_is_refused_naming_the_hook_and_ctx_emit() -> None:
    class Returns:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> str:
            return value

    with pytest.raises(DeclarationError) as caught:
        flezha.tasklet(Returns, name="returns")  # type: ignore[arg-type]
    assert "`run` is annotated to return `<class 'str'>`" in str(caught.value)
    assert "`ctx.emit`" in str(caught.value)


class ReturnsAnyway:
    def run(self, value: str, ctx: flezha.Ctx[None, str]):
        return "x"


def test_a_value_returned_at_run_time_fails_the_input_without_a_retry(
    fake_core: FakeCore,
) -> None:
    returns = flezha.tasklet(ReturnsAnyway, name="returns_anyway")  # type: ignore[arg-type]
    deps = solo_deps(returns, fake_core)
    effect = _effect(deps, "in")
    assert effect["verdict"] == "non_retryable"
    assert effect["message"] == (
        "NonRetryableError: returns_anyway: `run` returned a str; a value leaves a tasklet "
        "only through `ctx.emit`, so `run` returns None"
    )
    assert "outputs" not in effect


def test_the_harness_refuses_a_returned_value_with_the_runners_message() -> None:
    harness: TaskletTestHarness[None] = TaskletTestHarness(
        flezha.tasklet(ReturnsAnyway, name="returns_anyway")  # type: ignore[arg-type]
    )
    with pytest.raises(flezha.NonRetryableError) as caught:
        harness.feed("in")
    assert str(caught.value).startswith("returns_anyway: `run` returned a str;")
    assert harness.log == []


class Fires:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.timer_after(10)

    def on_timer(self, timer: dict[str, Any], ctx: flezha.Ctx[None, str]):
        if ctx.cell_key == "returns":
            return "fired"
        ctx.emit("fired")
        return None


def _timer_activation(cell: str) -> dict[str, Any]:
    activation = work_activation(2, "x", factory="solo")
    activation["cell_key"] = cell
    activation["items"][0].update(kind="timer", timer_kind="processing_time", scheduled_at=10)
    return activation


def test_a_timer_firing_emits_through_its_context_and_a_returned_value_fails_it(
    fake_core: FakeCore,
) -> None:
    deps = solo_deps(flezha.tasklet(Fires, name="fires"), fake_core)
    (emitted,) = answered(_timer_activation("emits"), deps)["effects"]
    (returned,) = answered(_timer_activation("returns"), deps)["effects"]
    assert (emitted["verdict"], emitted["outputs"][0]["payload"]) == ("ok", b"fired")
    assert returned["verdict"] == "non_retryable"
    assert "fires: `on_timer` returned a str" in returned["message"]


class RunWithoutContext:
    def run(self, value: str) -> None:
        del value


class TimerWithoutContext:
    def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
        ctx.timer_after(10)

    def on_timer(self, timer: dict[str, Any]) -> None:
        del timer


class CompensateWithoutContext:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)

    def compensate(self, outputs: list[Any]) -> None:
        del outputs


@pytest.mark.parametrize(
    ("target", "hook"),
    [
        (RunWithoutContext, "run"),
        (TimerWithoutContext, "on_timer"),
        (CompensateWithoutContext, "compensate"),
    ],
)
def test_a_hook_without_a_context_is_refused(target: type, hook: str) -> None:
    with pytest.raises(DeclarationError) as caught:
        flezha.tasklet(target, name="no_context")
    assert f"`{hook}` takes no context parameter" in str(caught.value)


class UndoesByReturning:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)

    def compensate(self, outputs: list[Any], ctx: flezha.Ctx[None, str]):
        return "undo"


def test_a_value_a_compensation_returns_fails_it_without_a_retry(fake_core: FakeCore) -> None:
    undoes = flezha.tasklet(UndoesByReturning, name="undoes")  # type: ignore[arg-type]
    activation = {**work_activation(6, factory="solo"), "kind": "compensate_run", "outputs": []}
    result = answered(activation, solo_deps(undoes, fake_core))
    assert (result["outcome"], result["non_retryable"]) == ("failed", True)
    assert result["error"] == (
        "NonRetryableError: undoes: `compensate` returned a str; a value leaves a tasklet only "
        "through `ctx.emit`, so `compensate` returns None"
    )


def test_the_retired_initial_state_hook_is_refused_naming_init_state() -> None:
    class Seeded:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            del value

        def initial_state(self, cell_key: str | None) -> dict[str, int]:
            return {}

    with pytest.raises(DeclarationError) as caught:
        flezha.tasklet(Seeded, name="seeded")
    assert "writes `initial_state`" in str(caught.value)
    assert "`init_state(self, cell_key)`" in str(caught.value)


def test_a_tasklet_that_emits_nothing_emits_nothing(fake_core: FakeCore) -> None:
    class Quiet:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            del value

    effect = _effect(solo_deps(flezha.tasklet(Quiet, name="quiet"), fake_core), "in")
    assert (effect["verdict"], effect["outputs"]) == ("ok", [])


# -- the short form's two bodies -------------------------------------------------


def test_a_function_short_form_emits_its_value_and_nothing_for_none(fake_core: FakeCore) -> None:
    @flezha.unbounded_operator(id="maybe", sink="out", codec="text")
    def maybe(value: str) -> str | None:
        return None if value == "skip" else value

    assert maybe.tasklet is not None
    assert maybe.tasklet("b") == "b"
    deps = deps_for(maybe, fake_core)
    skipped, emitted = answered(wire_work(3, "maybe", (0, b"skip"), (0, b"b")), deps)["effects"]
    assert skipped["outputs"] == []
    assert [output["payload"] for output in emitted["outputs"]] == [b"b"]


def test_a_class_short_form_emits_through_its_context(fake_core: FakeCore) -> None:
    @flezha.unbounded_operator(id="store_input", sink="sink", codec="text")
    class StoreInput:
        def run(self, value: str, ctx: flezha.Ctx[dict[str, list[str]], str]) -> None:
            ctx.change(value)
            if value != "quiet":
                ctx.emit(value)

        def apply_change(self, state: dict[str, list[str]], value: Any) -> None:
            state.setdefault("items", []).append(value)

    assert [node.name for node in StoreInput.spec.tasklets] == ["store_input"]
    deps = deps_for(StoreInput, fake_core)
    activation = wire_work(4, "store_input", (0, b"a"), (0, b"quiet"))
    emitted, quiet = answered(activation, deps)["effects"]
    assert [output["payload"] for output in emitted["outputs"]] == [b"a"]
    assert (len(emitted["changes"]), quiet["outputs"], len(quiet["changes"])) == (1, [], 1)


class Drain:
    def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
        del value


class BareContext:
    def run(self, value: str, ctx: flezha.Ctx) -> None:  # type: ignore[type-arg]
        del value


@pytest.mark.parametrize("body", [Drain, BareContext])
def test_a_class_short_form_whose_context_names_no_output_refuses_a_sink(body: type) -> None:
    with pytest.raises(DeclarationError) as caught:
        flezha.unbounded_operator(id="drain", sink="out")(body)
    assert caught.value.refusals == ["SinkWithoutOutput"]


# -- the clock follows from what the class writes -----------------------------------


class EventTimed:
    def run(self, trade: dict[str, int], ctx: flezha.Ctx[None, int]) -> None:
        ctx.emit(ctx.now())

    def event_time(self, trade: dict[str, int]) -> int:
        return trade["at"]


class Sequenced:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, int], None]) -> None:
        ctx.change(value)

    def apply_change(self, state: dict[str, int], value: Any) -> None:
        state[value] = 1


def test_the_clock_is_the_wall_unless_the_class_writes_another() -> None:
    wall = flezha.tasklet(Upcase, name="wall")
    event = flezha.tasklet(EventTimed, name="event")
    logical = flezha.tasklet(Sequenced, name="seq", logical_clock=True)
    assert [_record(solo(task))["clock"] for task in (wall, event, logical)] == [
        "wall",
        "event_time",
        "logical",
    ]
    harness: TaskletTestHarness[None] = TaskletTestHarness(wall)
    assert harness.feed("a", now=1_720_000_000_000).outputs == ["A"]


def test_an_event_time_tasklet_answers_the_extracted_time_at_the_crossing(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(solo(flezha.tasklet(EventTimed, name="event")), fake_core)
    payload = json.dumps({"at": 1_720_000_000_000}).encode()
    request = key_request(5, "solo", payload, want_keys=False, want_event_time=True)
    (prepared,) = answered(request, deps)["prepared"]
    assert prepared["event_time"] == 1_720_000_000_000


def test_two_clocks_are_refused_naming_both() -> None:
    with pytest.raises(DeclarationError) as caught:
        flezha.tasklet(EventTimed, name="both", logical_clock=True)
    assert "`logical_clock=True` and writes `event_time`" in str(caught.value)


def test_a_logical_clock_with_no_changes_is_still_unservable() -> None:
    with pytest.raises(DeclarationError) as caught:
        declare(solo(flezha.tasklet(Upcase, name="seq", logical_clock=True)))
    assert caught.value.refusals == ["UnservableClock"]


@pytest.mark.parametrize(
    "retired", [{"clock": "event_time"}, {"dynamic_cells": True}, {"idle_ttl_ms": 60_000}]
)
def test_a_retired_setting_is_an_unknown_keyword(retired: dict[str, Any]) -> None:
    with pytest.raises(TypeError, match=next(iter(retired))):
        flezha.tasklet(Upcase, name="t", **retired)
    with pytest.raises(TypeError, match=next(iter(retired))):
        flezha.unbounded_operator(id="t", **retired)


# -- the idle TTL is whole seconds ------------------------------------------------


def test_seconds_reach_the_record_in_milliseconds() -> None:
    ttl = flezha.tasklet(Upcase, name="ttl", idle_ttl_seconds=60)

    @flezha.unbounded_operator("ttl_op")
    def keyed(c: flezha.Channels) -> None:
        c.input("in").key_by(str).into(ttl).sink()

    assert (_record(keyed)["cell_mode"], _record(keyed)["idle_ttl_ms"]) == ("dynamic", 60_000)
    assert "idle_ttl_ms" not in _record(solo(flezha.tasklet(Upcase, name="none")))


@pytest.mark.parametrize("ttl", [0.2, 0, -1, True])
def test_an_idle_ttl_that_is_not_whole_positive_seconds_is_refused(ttl: Any) -> None:
    with pytest.raises(DeclarationError, match="idle_ttl_seconds"):
        flezha.tasklet(Upcase, name="ttl", idle_ttl_seconds=ttl)


# -- a sealed channel -----------------------------------------------------------


def test_sealing_a_channel_leaves_the_declaration_as_it_was() -> None:
    upcase = flezha.tasklet(Upcase, name="upcase")

    @flezha.unbounded_operator("open")
    def open_ended(c: flezha.Channels) -> None:
        c.input("in").into(upcase).sink("out")

    @flezha.unbounded_operator("open")
    def sealed(c: flezha.Channels) -> None:
        c.input("in").into(upcase).sink("out").sealed()

    assert graph.to_host_class_decl(sealed.spec) == graph.to_host_class_decl(open_ended.spec)
