"""A tasklet's part: what the host hands the core's part entry.

The numbering, the stamps and the drops are the core's; what is asserted here
is the host's half, over the fake core, and the core's own refusals in a local
run with no controller.
"""

from __future__ import annotations

from typing import Any

import pytest

import flezha
from conftest import FakeCore, answered, deps_for, work_activation
from flezha.errors import FlezhaError
from flezha.testing import TaskletTestHarness


def _recording(fake_core: FakeCore, seen: list[str], refusal: FlezhaError | None = None) -> None:
    def part(token: int, item: int, payload: bytes) -> None:
        if refusal is not None:
            raise refusal
        seen.append(f"{token}/{item}/{payload.decode()}")

    fake_core.part = part  # type: ignore[method-assign]


SEEN: list[str] = []


@flezha.tasklet(name="parts")
class Parts:
    """Sends the parts ``1..n`` for the input ``n``, then emits ``done:<n>``."""

    def run(self, n: int, ctx: flezha.Ctx[None, Any]) -> None:
        for part in range(1, n + 1):
            ctx.part({"part": part})
        SEEN.append(f"returned {n}")
        ctx.emit(f"done:{n}")

    def on_timer(self, timer: dict[str, Any], ctx: flezha.Ctx[None, Any]) -> None:
        ctx.part({"fired": timer["scheduled_at"]})


@flezha.relay("part_flow")
def _flow(c: flezha.Channels) -> None:
    c.input("in").into(Parts).output("answer")


@flezha.relay("part_steps", run={"steps": {"each": "per_input"}})
def _steps(c: flezha.Channels) -> None:
    c.input("in").into(Parts).output("answer")


@pytest.fixture(autouse=True)
def _clear() -> None:
    SEEN.clear()


def test_each_items_parts_carry_the_token_the_index_and_the_output_encoding(
    fake_core: FakeCore,
) -> None:
    _recording(fake_core, SEEN)
    deps = deps_for(_flow, fake_core)
    effects = answered(work_activation(7, 2, 1, factory="part_flow"), deps)["effects"]

    assert SEEN == [
        '7/0/{"part":1}',
        '7/0/{"part":2}',
        "returned 2",
        '7/1/{"part":1}',
        "returned 1",
    ]
    assert [[bytes(o["payload"]) for o in e["outputs"]] for e in effects] == [
        [b"done:2"],
        [b"done:1"],
    ], "a part is not an output"


def test_a_timer_handler_sends_a_part_under_its_own_item(fake_core: FakeCore) -> None:
    _recording(fake_core, SEEN)
    activation = work_activation(7, 0, 0, factory="part_flow")
    activation["items"][1].update(kind="timer", timer_kind="processing_time", scheduled_at=5)
    answered(activation, deps_for(_flow, fake_core))

    assert SEEN == ["returned 0", '7/1/{"fired":5}']


def test_a_per_input_steps_stage_sends_its_parts(fake_core: FakeCore) -> None:
    _recording(fake_core, SEEN)
    deps = deps_for(_steps, fake_core)
    (effect,) = answered(work_activation(7, 2, factory="part_steps"), deps)["effects"]

    assert effect["verdict"] == "ok"
    assert SEEN == ['7/0/{"part":1}', '7/0/{"part":2}', "returned 2"]


def test_a_refused_part_fails_the_invocation_as_any_raise_does(fake_core: FakeCore) -> None:
    refusal = FlezhaError("part.no_output", {"tasklet": "parts", "relay": "op", "channel": ""})
    _recording(fake_core, SEEN, refusal)
    deps = deps_for(_flow, fake_core)
    (effect,) = answered(work_activation(7, 1, factory="part_flow"), deps)["effects"]

    assert effect["verdict"] == "retryable"
    assert effect["message"].endswith("(part.no_output)")
    assert "outputs" not in effect


def test_the_harness_encodes_a_part_and_drops_it() -> None:
    harness = TaskletTestHarness(Parts)
    assert harness.feed(2).outputs == ["done:2"]

    @flezha.tasklet(name="unencodable")
    class Unencodable:
        def run(self, value: Any, ctx: flezha.Ctx[None, Any]) -> None:
            ctx.part(object())

    with pytest.raises(TypeError):
        TaskletTestHarness(Unencodable).feed(1)


@flezha.tasklet(name="refused_part")
class RefusedPart:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        try:
            ctx.part(value)
        except FlezhaError as error:
            SEEN.append(error.code)
        ctx.emit(value)


@flezha.tasklet(name="part_drain")
class PartDrain:
    def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
        pass


@flezha.relay("part_to_tasklet")
def _to_tasklet(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(RefusedPart).into(PartDrain)


@flezha.relay("part_local")
def _local(c: flezha.Channels) -> None:
    c.input("in").into(Parts).output("answer")


def test_a_local_run_drops_every_part_and_releases_the_output_alone() -> None:
    released: list[bytes] = []
    with flezha.LocalRelay.start(_local) as run:
        run.on_output("answer", lambda output: released.append(bytes(output.payload)))
        run.feed("in", b"3")
        run.run_until_idle(10_000)
    assert released == [b"done:3"]


def test_the_core_refuses_a_part_whose_channel_ends_in_no_output() -> None:
    with flezha.LocalRelay.start(_to_tasklet) as run:
        run.feed("in", "p")
        run.run_until_idle(10_000)
    assert SEEN == ["part.no_output"]
