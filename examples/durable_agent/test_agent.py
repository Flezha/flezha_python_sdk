"""The example's own tests, with no controller running.

The harness drives the same dispatch the runner does, so an agent's durability is testable at the speed of a
unit test — which is the point of shipping the harness at all.
"""

from __future__ import annotations

import pytest

import flezha
from agent import Conversation, Session, agent, agent_session, durable_agent, session_turn
from flezha import graph, native
from flezha.testing import TaskletTestHarness


def harness() -> TaskletTestHarness[Conversation]:
    return TaskletTestHarness(agent, cell_key="user-1")


def test_a_turn_is_recorded_and_answered() -> None:
    h = harness()
    outcome = h.feed("what is the weather")
    assert h.state.turns[0].prompt == "what is the weather"
    assert h.state.spent_tokens > 0
    assert outcome.outputs[0].startswith("answered(")


def test_the_transcript_survives_a_restart() -> None:
    """What the fold produces from the log IS what a restart produces."""
    h = harness()
    for value in ["first", "second", "book table-9"]:
        h.feed(value)
    assert h.replay_equals()
    assert h.state.booked == ["table-9"]


def test_an_unrecorded_mutation_is_refused_rather_than_lost() -> None:
    h = harness()
    h.feed("one")
    with pytest.raises(flezha.FlezhaError, match="spent_tokens"):
        h.state.spent_tokens = 0


def test_an_empty_prompt_parks_instead_of_burning_the_retry_budget() -> None:
    """A refusal the model will never accept differently is not worth the ladder."""
    with pytest.raises(flezha.NonRetryableError, match="empty prompt"):
        harness().feed("   ")


def test_stable_ids_reproduce_across_a_replay() -> None:
    first = harness().feed("same prompt")
    second = harness().feed("same prompt")
    assert first.outputs == second.outputs


def test_compensation_cancels_what_the_turn_booked() -> None:
    h = harness()
    h.feed("book table-9")
    h.compensate([])
    assert h.state.booked == ["table-9"]


def test_a_session_keeps_its_goal_and_turns_and_records_nothing_on_done() -> None:
    h: TaskletTestHarness[Session] = TaskletTestHarness(session_turn)
    for value in [{"goal": "book a table"}, "hello", "again"]:
        h.feed(value)
    assert (h.state.goal, h.state.turns) == ("book a table", 2)
    assert h.replay_equals()
    ending = h.feed("done")
    assert (ending.outputs, ending.changes) == ([], [])


@pytest.mark.parametrize("relay", [durable_agent, agent_session])
def test_the_graph_registers(relay: flezha.Relay) -> None:
    core = native.Core.create()
    try:
        graph.validate(relay.spec, core)
    finally:
        core.free()
    assert graph.to_host_class_decl(relay.spec)["factory_id"] == relay.factory_id
