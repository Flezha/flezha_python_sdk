"""An operator inside an operator, run locally: a flow class whose keyed edge
ends in a steps class runs one stepped instance of it per key, each with its
own state, and the instance's exit output continues on the outer graph; a
flow class is inlined under prefixed names."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pytest

from flezha import Channels, Ctx, FlezhaError, LocalOperator, operator, state, tasklet


@state
@dataclass
class Turns:
    turns: int = 0


@dataclass
class OneMore:
    by: int = 1


@tasklet(name="turn", state=Turns, change=OneMore)
class Turn:
    """One turn of a session: ``key:text`` answered as ``key#turns:text``."""

    def run(self, line: str, ctx: Ctx[Turns, Any]) -> None:
        key, _, text = line.partition(":")
        ctx.change(OneMore())
        ctx.emit(f"{key}#{ctx.state.turns}:{text}")

    def init_state(self, cell_key: str | None) -> Turns:
        return Turns()

    def apply_change(self, state: Turns, change: OneMore) -> None:
        state.turns += change.by


@tasklet(name="reply")
class Reply:
    def run(self, line: str, ctx: Ctx[None, Any]) -> None:
        ctx.emit(line)


@tasklet(name="trim")
class Trim:
    def run(self, line: str, ctx: Ctx[None, Any]) -> None:
        ctx.emit(line.strip())


@tasklet(name="upper")
class Upper:
    def run(self, line: str, ctx: Ctx[None, Any]) -> None:
        ctx.emit(line.upper())


@operator("inner_session_python", run={"steps": {"each": "per_input"}}, durable=False)
def session(c: Channels) -> None:
    c.input("turns", codec="text").into(Turn).named("answer")


@operator("inner_gateway_python")
def gateway(c: Channels) -> None:
    c.input("requests", codec="text").key_by(lambda line: line.split(":")[0]).into(
        session, max_cells=2
    ).into(Reply).sink("out", codec="text")


@operator("inner_cleanup_python")
def cleanup(c: Channels) -> None:
    c.input("raw", codec="text").into(Trim).into(Upper)


@operator("inner_inlining_python")
def inlining(c: Channels) -> None:
    c.input("requests", codec="text").into(cleanup).into(Reply).sink("out", codec="text")


@operator("inner_two_entries_python")
def two_entries(c: Channels) -> None:
    c.input("a").into(Trim)
    c.input("b").into(Upper)


@operator("inner_ambiguous_entry_python")
def ambiguous_entry(c: Channels) -> None:
    c.input("in").into(two_entries).into(Reply)


@operator("inner_steps_outer_python", run={"steps": {"each": "per_input"}}, durable=False)
def steps_outer(c: Channels) -> None:
    c.input("in", codec="text").into(session).into(Reply)


def _run(declared: Any, lines: list[str], count: int) -> list[str]:
    released: list[Any] = []
    with LocalOperator.start(declared) as run:
        run.on_sink("out", released.append)
        for line in lines:
            run.feed("requests", line)
        run.run_until_idle(5_000)
    assert len(released) == count, released
    return sorted(output.payload.decode() for output in released)


def test_every_key_runs_its_own_scope_and_the_exit_continues_the_outer_graph() -> None:
    assert _run(gateway, ["alice:hi", "bob:yo", "alice:again"], 3) == [
        "alice#1:hi",
        "alice#2:again",
        "bob#1:yo",
    ]


def test_a_third_key_past_max_cells_is_dropped() -> None:
    assert _run(gateway, ["alice:hi", "bob:yo", "carol:hey"], 2) == ["alice#1:hi", "bob#1:yo"]


def test_a_flow_class_is_inlined_under_prefixed_names() -> None:
    assert _run(inlining, ["  hi  "], 1) == ["HI"]
    names = sorted(node.name for node in inlining.spec.tasklets)
    assert names == ["inner_cleanup_python.trim", "inner_cleanup_python.upper", "reply"]
    assert all(node.embeds is None for node in inlining.spec.tasklets)


def test_an_inlined_class_with_two_entries_and_no_input_is_refused_naming_both() -> None:
    with pytest.raises(FlezhaError) as caught:
        _ = ambiguous_entry.spec
    assert caught.value.code == "topology.embedded_entry_ambiguous"
    assert caught.value.details == {"inner": "inner_two_entries_python", "channels": "a, b"}


def test_a_steps_class_embedding_a_steps_class_is_refused_naming_both() -> None:
    with pytest.raises(FlezhaError) as caught:
        LocalOperator.start(steps_outer)
    assert caught.value.code == "topology.nested_steps", caught.value
    assert "inner_steps_outer_python" in str(caught.value)
    assert "inner_session_python" in str(caught.value)


@operator("inner_after_a_tasklet_python")
def after_a_tasklet(c: Channels) -> None:
    c.input("requests", codec="text").into(Trim).key_by(lambda line: line.split(":")[0]).into(
        session
    ).into(Reply).sink("out", codec="text")


@operator("inner_sink_after_python")
def sink_after(c: Channels) -> None:
    c.input("requests", codec="text").into(session).sink("out", codec="text")


def test_a_value_a_tasklet_emits_enters_the_scope_in_its_entry_codec() -> None:
    assert _run(after_a_tasklet, ["  alice:hi  "], 1) == ["alice#1:hi"]


def test_a_sink_right_after_an_embedded_class_is_refused() -> None:
    with pytest.raises(FlezhaError) as caught:
        LocalOperator.start(sink_after)
    assert caught.value.code == "topology.invalid_setting"
    assert caught.value.details["value"] == "out"
