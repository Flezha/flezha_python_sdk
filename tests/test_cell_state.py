"""A cell belongs to one operator.

One registered class serves every operator of its factory on a runner, such as
each instance of a steps operator or each partition of a stream, and each has
cells of its own under the same tasklet and cell key. Every path that reads or
writes a cell's state is driven here with two operators side by side.
"""

from __future__ import annotations

import json
from typing import Any

import flezha
from conftest import FakeCore, answered, deps_for, work_activation
from flezha import codec
from flezha.dispatch import DispatchDeps
from flezha.testing import TaskletTestHarness


class Kept:
    """Keeps what it was sent, and undoes by emitting what its own cell holds."""

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        ctx.change(value)

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"seen": []}

    def apply_change(self, state: dict[str, Any], change: Any) -> None:
        state["seen"].append(change)

    def compensate(self, outputs: list[Any], ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        ctx.emit(list(ctx.state["seen"]))


kept = flezha.tasklet(Kept, name="kept")


@flezha.operator("kept_operator")
def kept_operator(c: flezha.Channels) -> None:
    c.input("input").into(kept).sink("out")


def _activation(kind: str, operator: str, **fields: Any) -> dict[str, Any]:
    return {
        "token": 1,
        "operator": operator,
        "tasklet": 0,
        "cell_key": None,
        "change_seq": 0,
        "factory": "kept_operator",
        "kind": kind,
        **fields,
    }


def _work(deps: DispatchDeps, operator: str, value: str) -> None:
    activation = {**work_activation(1, value, factory="kept_operator"), "operator": operator}
    (effect,) = answered(activation, deps)["effects"]
    assert effect["verdict"] == "ok", effect


def _read(deps: DispatchDeps, operator: str) -> Any:
    return json.loads(answered(_activation("live_state_read", operator), deps)["json"])


def test_two_operators_of_one_factory_keep_their_own_cell(fake_core: FakeCore) -> None:
    deps = deps_for(kept_operator, fake_core)
    _work(deps, "order_a", "a")
    _work(deps, "order_b", "b")
    _work(deps, "order_a", "c")

    assert _read(deps, "order_a") == {"seen": ["a", "c"]}
    assert _read(deps, "order_b") == {"seen": ["b"]}


def test_a_restore_and_an_applied_state_reach_only_their_own_operator(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(kept_operator, fake_core)
    restore = _activation(
        "restore_fold", "order_a", snapshot=None, changes=[{"bytes": codec.encode("r")}]
    )
    restored = answered(restore, deps)
    applied = answered(_activation("apply_state", "order_b", state_json=b'{"seen": ["m"]}'), deps)

    assert restored["outcome"] == "restored"
    assert applied["outcome"] == "state_applied"
    assert _read(deps, "order_a") == {"seen": ["r"]}
    assert _read(deps, "order_b") == {"seen": ["m"]}
    assert _read(deps, "order_c") == {"seen": []}


def test_a_compensation_reads_its_own_operators_cell(fake_core: FakeCore) -> None:
    deps = deps_for(kept_operator, fake_core)
    _work(deps, "order_a", "a")
    _work(deps, "order_b", "b")

    undo = answered(_activation("compensate_run", "order_b", outputs=[]), deps)

    (effect,) = undo["effects"]
    assert json.loads(effect["outputs"][0]["payload"]) == ["b"]


def test_a_harness_restore_fills_the_cell_it_reads() -> None:
    harness: TaskletTestHarness[dict[str, Any]] = TaskletTestHarness(kept, operator="order_a")
    harness.restore(changes=[codec.encode("r")])

    assert harness.state == {"seen": ["r"]}
