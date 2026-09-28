"""The state-mutation guard, and the runtime path it deliberately leaves open."""

from __future__ import annotations

import threading
from dataclasses import FrozenInstanceError, dataclass, field
from typing import Any, TypedDict

import pytest

import flezha
from conftest import Bumped, Counter, count_up
from flezha import cell_state as state_module
from flezha.errors import FlezhaError
from flezha.testing import TaskletTestHarness


def test_direct_assignment_is_refused_naming_the_field_and_the_path() -> None:
    counter = Counter()
    with pytest.raises(FlezhaError) as caught:
        counter.count = 5
    message = str(caught.value)
    assert "count" in message
    assert "ctx.change" in message


def test_construction_is_not_mutation() -> None:
    """A state object has to be built before it can be guarded."""
    assert Counter(count=3).count == 3
    assert Counter().seen == []


def test_the_change_path_succeeds_and_is_recorded() -> None:
    harness: TaskletTestHarness[Counter] = TaskletTestHarness(count_up)
    outcome = harness.feed("abc")
    assert harness.state.count == 3
    assert outcome.changes == [Bumped(by=3)]
    assert harness.replay_equals()


def test_recorded_changes_reproduce_the_state_on_replay() -> None:
    harness: TaskletTestHarness[Counter] = TaskletTestHarness(count_up)
    for value in ("a", "bb", "ccc"):
        harness.feed(value)
    live = harness.state
    rebuilt = harness.restore(changes=list(harness.log))
    assert rebuilt == live


def test_a_snapshot_plus_a_change_log_rebuilds_the_state() -> None:
    harness: TaskletTestHarness[Counter] = TaskletTestHarness(count_up)
    harness.feed("ab")
    snapshot = harness.snapshot()
    harness.feed("cde")
    tail = harness.log[1:]

    fresh: TaskletTestHarness[Counter] = TaskletTestHarness(count_up)
    assert fresh.restore(snapshot=snapshot, changes=tail) == harness.state


def test_the_runtime_permission_is_thread_local() -> None:
    """The runtime's own path opens the guard on one thread and not on others."""
    seen: list[bool] = []

    def observe() -> None:
        seen.append(state_module._mutation_permitted())

    with state_module.runtime_mutation():
        assert state_module._mutation_permitted()
        thread = threading.Thread(target=observe)
        thread.start()
        thread.join()
    assert seen == [False]
    assert not state_module._mutation_permitted()


@flezha.state
@dataclass(frozen=True)
class Frozen:
    """A class that refuses assignment on its own terms."""

    value: int = 0


def test_the_guard_refuses_before_the_class_it_wraps_can() -> None:
    """A frozen dataclass gains our refusal; its own never runs while sealed."""
    with pytest.raises(FlezhaError):
        Frozen().value = 1  # type: ignore[misc]


def test_the_guard_delegates_to_the_class_it_wraps_inside_the_runtime_window() -> None:
    """Inside the runtime window the class's own refusal is what answers."""
    frozen = Frozen()
    with state_module.runtime_mutation(), pytest.raises(FrozenInstanceError):
        frozen.value = 1  # type: ignore[misc]


def test_decorating_twice_is_idempotent() -> None:
    once = flezha.state(Counter)
    assert once is Counter


def test_a_declared_state_survives_a_nested_fold() -> None:
    """The permission nests, so a restore that folds changes stays open."""

    @flezha.state
    @dataclass
    class Nested:
        items: list[str] = field(default_factory=list)
        depth: int = 0

    class Folding:
        def run(self, value: str, ctx: flezha.Ctx[Nested, None]) -> None:
            ctx.change({"item": value})

        def init_state(self, key: str | None) -> Nested:
            return Nested()

        def apply_change(self, state: Nested, change: dict[str, str]) -> None:
            state.items = [*state.items, change["item"]]
            state.depth += 1

    task = flezha.tasklet(Folding, name="folding")
    harness: TaskletTestHarness[Nested] = TaskletTestHarness(task)
    harness.feed("a")
    harness.feed("b")
    assert harness.state == Nested(items=["a", "b"], depth=2)
    assert harness.replay_equals()


# -- where a new cell's state comes from ---------------------------------------


@dataclass
class Seen:
    words: list[str] = field(default_factory=list)


class SeesWords:
    def run(self, word: str, ctx: flezha.Ctx[Seen, str]) -> None:
        ctx.change(word)
        ctx.emit(word)

    def apply_change(self, state: Seen, word: Any) -> None:
        state.words.append(word)


class ByCell:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, str], None]) -> None:
        del value

    def init_state(self, cell_key: str | None) -> dict[str, str]:
        return {"cell": str(cell_key)}

    def cells_for(self, value: str) -> list[str]:
        return [value]


class Totals:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, int], None]) -> None:
        ctx.change(value)

    def apply_change(self, state: dict[str, int], value: Any) -> None:
        state[value] = state.get(value, 0) + 1


@dataclass
class Needs:
    seed: int


class NeedsSeed:
    def run(self, value: str, ctx: flezha.Ctx[Needs, None]) -> None:
        del value


def test_a_new_cell_starts_from_the_state_class_its_context_names() -> None:
    harness: TaskletTestHarness[Seen] = TaskletTestHarness(
        flezha.tasklet(SeesWords, name="sees_words")
    )
    assert harness.state == Seen()
    assert harness.feed("a").outputs == ["a"]
    assert harness.state == Seen(words=["a"])


def test_a_written_init_state_receives_the_cell_key() -> None:
    task = flezha.tasklet(ByCell, name="by_cell", cell_keys=["a", "b"])
    harness: TaskletTestHarness[dict[str, str]] = TaskletTestHarness(task, cell_key="b")
    assert harness.state == {"cell": "b"}


def test_a_parameterised_builtin_starts_empty() -> None:
    harness: TaskletTestHarness[dict[str, int]] = TaskletTestHarness(
        flezha.tasklet(Totals, name="totals")
    )
    assert harness.state == {}
    harness.apply_inputs(["x", "y", "x"])
    assert harness.state == {"x": 2, "y": 1}


def test_an_unbuildable_state_class_is_refused_naming_it_and_init_state() -> None:
    with pytest.raises(flezha.DeclarationError) as caught:
        flezha.tasklet(NeedsSeed, name="needs_seed")
    assert "state class `Needs` cannot be built with no arguments" in str(caught.value)
    assert "`init_state(self, cell_key)`" in str(caught.value)


def test_a_restore_folds_from_the_state_class_default() -> None:
    task = flezha.tasklet(SeesWords, name="sees_words")
    live: TaskletTestHarness[Seen] = TaskletTestHarness(task)
    live.apply_inputs(["a", "b", "c"])
    restored: TaskletTestHarness[Seen] = TaskletTestHarness(task)
    assert restored.restore(changes=live.log) == live.state == Seen(words=["a", "b", "c"])


class Window(TypedDict):
    prices: list[float]


class PriceSeen(TypedDict):
    price: float


class Windows:
    def run(self, price: float, ctx: flezha.Ctx[Window, None]) -> None:
        ctx.change(PriceSeen(price=price))

    def init_state(self, cell_key: str | None) -> Window:
        return Window(prices=[])

    def apply_change(self, window: Window, seen: PriceSeen) -> None:
        window["prices"].append(seen["price"])


def test_a_typed_dict_state_and_change_are_plain_dicts() -> None:
    task = flezha.tasklet(Windows, name="windows")
    assert (task.state_codec, task.change_codec) == (None, None)
    harness: TaskletTestHarness[Window] = TaskletTestHarness(task)
    harness.apply_inputs([1.5, 2.5])
    assert harness.state == {"prices": [1.5, 2.5]}
    assert harness.replay_equals()
