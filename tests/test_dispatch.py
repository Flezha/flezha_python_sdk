"""One activation in, one answer out — and never none.

``answer`` is a pure function of the activation and the registry, so every
mapping below asserts what it returns. The handout contract — an activation
that returns unanswered retires its cell for the life of the process — is
asserted against a real core, whose refusal of a token it never handed out is
what both the completion and the abandon meet.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest

import app
import flezha
from conftest import (
    FakeCore,
    answered,
    compensating,
    demo,
    deps_for,
    echo,
    key_request,
    solo_deps,
    wire_work,
    work_activation,
)
from flezha import codec, native
from flezha.dispatch import DispatchDeps, answer, dispatch_now
from flezha.errors import NonRetryableError

fixtures = app.module("fixtures")
fixture_support = app.module("fixture_support")


@pytest.fixture
def deps(fake_core: FakeCore) -> DispatchDeps:
    return deps_for(demo, fake_core)


def test_an_input_is_answered_with_its_effects(deps: DispatchDeps) -> None:
    result = answered(work_activation(1, "abc"), deps)
    assert result["outcome"] == "work"
    (effect,) = result["effects"]
    assert effect["verdict"] == "ok"
    assert effect["consumed_offset"] == 1
    assert codec.decode(effect["outputs"][0]["payload"]) == "abc"
    assert effect["complete"] is None


def test_an_unknown_factory_is_a_non_retryable_failure(deps: DispatchDeps) -> None:
    result = answered(work_activation(2, "x", factory="absent"), deps)
    assert result["outcome"] == "failed"
    assert result["non_retryable"] is True
    assert "absent" in result["error"]


def test_a_handler_error_becomes_a_retryable_verdict(fake_core: FakeCore) -> None:
    @flezha.tasklet(name="boom")
    class Boom:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            raise ValueError("nope")

    deps = solo_deps(Boom, fake_core)
    (effect,) = answered(work_activation(3, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "retryable"
    assert effect["message"] == "ValueError: nope"


def test_a_non_retryable_error_says_so(fake_core: FakeCore) -> None:
    @flezha.tasklet(name="refuse")
    class Refuse:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            raise NonRetryableError("bad input")

    deps = solo_deps(Refuse, fake_core)
    (effect,) = answered(work_activation(4, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "non_retryable"


def test_a_batch_stops_at_the_first_failure(fake_core: FakeCore) -> None:
    seen: list[str] = []

    @flezha.tasklet(name="picky")
    class Picky:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            seen.append(value)
            if value == "bad":
                raise ValueError("nope")
            ctx.emit(value)

    deps = solo_deps(Picky, fake_core)
    effects = answered(work_activation(5, "ok", "bad", "never", factory="solo"), deps)["effects"]
    # Items after the failure are re-handed by the core, so the batch stops
    # rather than reporting effects the cell never saw.
    assert seen == ["ok", "bad"]
    assert [effect["verdict"] for effect in effects] == ["ok", "retryable"]


def test_a_failure_inside_complete_abandons_rather_than_retiring_the_cell(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The guard, against the collaborator that decides it.

    A real core created without ``start`` never handed this token out, so it
    refuses the completion — and refuses the abandon the guard falls back to.
    Neither refusal escapes, and both are reported.
    """
    core = native.Core.create()
    try:
        with caplog.at_level("DEBUG", logger="flezha"):
            dispatch_now(work_activation(6, "x"), core, deps_for(demo, core))
    finally:
        core.free()
    assert "(runtime.unknown_activation_token)" in caplog.text
    assert "abandoning activation 6 failed" in caplog.text


def test_an_async_handler_is_driven_off_an_event_loop(fake_core: FakeCore) -> None:
    @flezha.tasklet(name="slow")
    class Slow:
        async def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            await asyncio.sleep(0)
            ctx.emit(value.upper())

    deps = solo_deps(Slow, fake_core)
    (effect,) = answered(work_activation(7, "hi", factory="solo"), deps)["effects"]
    assert effect["outputs"][0]["payload"] == b"HI"


def test_a_filtered_input_is_consumed_and_reaches_no_handler(fake_core: FakeCore) -> None:
    seen: list[str] = []

    @flezha.tasklet(name="only_long")
    class OnlyLong:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            seen.append(value)
            ctx.emit(value)

    @flezha.relay("filtered")
    def filtered(c: flezha.ChannelBuilder) -> None:
        c.input("in").filter(lambda value: len(value) > 2).into(OnlyLong).output()

    deps = deps_for(filtered, fake_core)

    (effect,) = answered(work_activation(8, "ab", factory="filtered"), deps)["effects"]
    assert seen == []
    # The offset still advances: the item is consumed, not retried.
    assert effect["verdict"] == "ok"
    assert effect["consumed_offset"] == 8
    assert effect["outputs"] == []


def test_a_key_request_answers_one_entry_per_payload(fake_core: FakeCore) -> None:
    @flezha.tasklet(name="routed")
    class Routed:
        def run(self, value: dict[str, str], ctx: flezha.Ctx[None, dict[str, str]]) -> None:
            ctx.emit(value)

    @flezha.relay("routed_op")
    def routed_op(c: flezha.ChannelBuilder) -> None:
        c.input("in").key_by(lambda value: value["k"]).into(Routed).output()

    deps = deps_for(routed_op, fake_core)

    result = answered(key_request(9, "routed_op", b'{"k": "a"}', b'{"k": "b"}'), deps)
    assert result["outcome"] == "prepared"
    assert [keys_answered(answer) for answer in result["prepared"]] == [["a"], ["b"]]


def test_a_key_request_answers_the_keyed_consumer_behind_an_unkeyed_one(
    fake_core: FakeCore,
) -> None:
    """The keys are the keyed consumer's, whatever an unkeyed consumer declared
    before it filters out."""

    @flezha.relay("split_keyed")
    def split_keyed(c: flezha.ChannelBuilder) -> None:
        entry = c.input("in", codec="text")
        entry.filter(lambda value: value == "drop").into(echo).output("dropped")
        entry.filter(lambda value: value != "drop").key_by(fixture_support.prefix).into(
            fixtures.transform_label
        ).output("out")

    crossing = key_request(11, "split_keyed", b"a:1", b"drop")
    prepared = answered(crossing, deps_for(split_keyed, fake_core))["prepared"]
    assert [keys_answered(answer) for answer in prepared] == [["a"], []]


def test_a_per_payload_refusal_does_not_fail_the_whole_crossing(fake_core: FakeCore) -> None:
    @flezha.tasklet(name="routed")
    class Routed:
        def run(self, value: dict[str, str], ctx: flezha.Ctx[None, dict[str, str]]) -> None:
            ctx.emit(value)

    def key_of(value: dict[str, str]) -> str:
        if value["k"] == "bad":
            raise ValueError("no key")
        return value["k"]

    @flezha.relay("picky_keys")
    def picky_keys(c: flezha.ChannelBuilder) -> None:
        c.input("in").key_by(key_of).into(Routed).output()

    deps = deps_for(picky_keys, fake_core)

    crossing = key_request(10, "picky_keys", b'{"k": "bad"}', b'{"k": "ok"}')
    answers = answered(crossing, deps)["prepared"]
    assert "no key" in answers[0]["error"]
    assert keys_answered(answers[1]) == ["ok"]


def test_a_live_state_read_answers_json(deps: DispatchDeps) -> None:
    # Straight to the stateful tasklet: an answer routes no edges, so an
    # input to `echo` would never reach `count_up`.
    answered(work_activation(11, "abc", tasklet=1), deps)
    result = answered(
        {
            "token": 12,
            "relay": "op",
            "tasklet": 1,
            "change_seq": 0,
            "factory": "demo",
            "kind": "live_state_read",
        },
        deps,
    )
    assert result["outcome"] == "state_json"
    assert codec.from_state_json(result["json"]) == {"count": 3, "seen": ["3"]}


def test_a_snapshot_and_a_restore_fold_round_trip(deps: DispatchDeps) -> None:
    answered(work_activation(13, "abcd", tasklet=1), deps)
    snapshot = answered(
        {
            "token": 14,
            "relay": "op",
            "tasklet": 1,
            "change_seq": 0,
            "factory": "demo",
            "kind": "snapshot_request",
        },
        deps,
    )["state"]

    restored = answered(
        {
            "token": 15,
            "relay": "op",
            "tasklet": 1,
            "cell_key": "other",
            "change_seq": 0,
            "factory": "demo",
            "kind": "restore_fold",
            "snapshot": {"bytes": snapshot},
            "changes": [],
        },
        deps,
    )
    assert restored["outcome"] == "restored"


def test_an_unknown_activation_kind_is_a_non_retryable_failure(deps: DispatchDeps) -> None:
    activation = work_activation(16, "x")
    activation["kind"] = "invented"
    result = answered(activation, deps)
    assert result["outcome"] == "failed"
    assert result["non_retryable"] is True


def test_reserved_metadata_is_refused_naming_the_key(fake_core: FakeCore) -> None:
    @flezha.tasklet(name="stamped")
    class Stamped:
        def run(self, value: str, ctx: flezha.Ctx[None, Any]) -> None:
            ctx.emit(value, metadata={"flz.seq": "1"})

    deps = solo_deps(Stamped, fake_core)
    (effect,) = answered(work_activation(17, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "retryable"
    assert "flz.seq" in effect["message"]


def test_a_keyed_channel_with_no_key_is_refused_naming_it(fake_core: FakeCore) -> None:
    @flezha.tasklet(name="keyless")
    class Keyless:
        def run(self, value: str, ctx: flezha.Ctx[None, Any]) -> None:
            ctx.emit(value)

    @flezha.relay("keyed_sink")
    def keyed_sink(c: flezha.ChannelBuilder) -> None:
        c.input("in").into(Keyless).keyed().output("out")

    deps = deps_for(keyed_sink, fake_core)
    (effect,) = answered(work_activation(18, "x", factory="keyed_sink"), deps)["effects"]
    assert "out" in effect["message"]


# -- a tasklet ending its relay ------------------------------------------------
#
# The host only carries the request in the item's effect. Ending the instance
# after the iteration commits is the core's.

_RESULTS: dict[str, Any] = {"text": "3", "bytes": b"\x00raw", "json": {"turns": 3}, "none": None}


@flezha.tasklet(name="finisher")
class Finisher:
    def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
        ctx.complete("replaced")
        ctx.complete(_RESULTS[value])


@flezha.relay("finishing_steps", run="steps")
def _finishing_steps(c: flezha.ChannelBuilder) -> None:
    c.input("in").into(Finisher).output()


@flezha.relay("finishing_flow")
def _finishing_flow(c: flezha.ChannelBuilder) -> None:
    c.input("in").into(Finisher).output()


@pytest.mark.parametrize(
    ("value", "carried"),
    [("text", b"3"), ("bytes", b"\x00raw"), ("json", b'{"turns":3}'), ("none", b"")],
)
def test_complete_carries_the_latest_result_in_the_effect(
    fake_core: FakeCore, value: str, carried: bytes
) -> None:
    deps = deps_for(_finishing_steps, fake_core)
    (effect,) = answered(work_activation(19, value, factory="finishing_steps"), deps)["effects"]
    assert effect["verdict"] == "ok"
    assert effect["complete"] == carried


def test_a_flow_tasklet_completing_is_carried_the_same_way(fake_core: FakeCore) -> None:
    deps = deps_for(_finishing_flow, fake_core)
    (effect,) = answered(work_activation(20, "text", factory="finishing_flow"), deps)["effects"]
    assert effect["complete"] == b"3"


def test_a_failed_item_carries_no_completion(fake_core: FakeCore) -> None:
    @flezha.tasklet(name="fails_after_completing")
    class FailsAfterCompleting:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            ctx.complete(value)
            raise ValueError("after completing")

    deps = solo_deps(FailsAfterCompleting, fake_core)
    (effect,) = answered(work_activation(21, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "retryable"
    assert "complete" not in effect


# -- timers inside a steps relay ------------------------------------------------
#
# A steps tasklet arms and cancels timers exactly as a flow relay one does; only
# a compensate hook refuses them.


@flezha.tasklet(name="timer_setter")
class TimerSetter:
    def run(self, call: str, ctx: flezha.Ctx[None, None]) -> None:
        if call == "after":
            ctx.timer_after(60_000)
        elif call == "event_time":
            ctx.timer_at_event_time(60_000)
        else:
            ctx.cancel_timer("processing_time", 60_000)


@flezha.relay("timer_steps", run="steps")
def _timer_steps(c: flezha.ChannelBuilder) -> None:
    c.input("in").into(TimerSetter)


@flezha.relay("timer_flow")
def _timer_flow(c: flezha.ChannelBuilder) -> None:
    c.input("in").into(TimerSetter)


@pytest.mark.parametrize(
    ("call", "operation"),
    [
        ("after", {"kind": "processing_time", "scheduled_at": 60_000, "register": True}),
        ("event_time", {"kind": "event_time", "scheduled_at": 60_000, "register": True}),
        ("cancel", {"kind": "processing_time", "scheduled_at": 60_000, "register": False}),
    ],
)
def test_a_timer_call_inside_a_steps_reaches_the_item_effect(
    fake_core: FakeCore, call: str, operation: dict[str, Any]
) -> None:
    deps = deps_for(_timer_steps, fake_core)
    (effect,) = answered(work_activation(22, call, factory="timer_steps"), deps)["effects"]
    assert effect["verdict"] == "ok"
    assert effect["timers"] == [operation]


class _ArmsOnUndo:
    def run(self, value: str, ctx: flezha.Ctx[None, Any]) -> None:
        ctx.emit(value)

    def compensate(self, outputs: list[Any], ctx: flezha.Ctx[None, Any]) -> None:
        ctx.timer_after(1_000)


def test_a_timer_call_inside_a_compensate_hook_is_refused(fake_core: FakeCore) -> None:
    deps = solo_deps(flezha.tasklet(_ArmsOnUndo, name="arms_on_undo"), fake_core)
    activation = {
        "token": 24,
        "relay": "op",
        "tasklet": 0,
        "change_seq": 0,
        "factory": "solo",
        "kind": "compensate_run",
        "outputs": [],
    }
    result = answered(activation, deps)
    assert result["outcome"] == "failed"
    assert result["error"].startswith("FlezhaError: ")
    assert result["error"].endswith("(runtime.timers_unavailable)")


def test_a_flow_tasklet_still_registers_its_timer(fake_core: FakeCore) -> None:
    deps = deps_for(_timer_flow, fake_core)
    (effect,) = answered(work_activation(23, "after", factory="timer_flow"), deps)["effects"]
    assert effect["verdict"] == "ok"
    assert [timer["register"] for timer in effect["timers"]] == [True]


def test_a_compensation_runs_over_the_recorded_outputs(fake_core: FakeCore) -> None:
    seen: list[Any] = []
    deps = solo_deps(compensating(seen), fake_core)
    result = answered(
        {
            "token": 20,
            "relay": "op",
            "tasklet": 0,
            "change_seq": 0,
            "factory": "solo",
            "kind": "compensate_run",
            "outputs": [codec.encode("a"), codec.encode("b")],
        },
        deps,
    )
    assert seen == ["a", "b"]
    assert result["outcome"] == "work"


# -- the structured-steps emit arm ------------------------------------------
#
# One released value is ONE edge output on a steps class, untransformed and
# unkeyed, naming the producing channel and its first consumer: the driver fans
# it out from the journaled handoff. On a flow class the same topology
# releases one output per consumer, transformed and keyed. The plugin output
# rides alongside either way.


@flezha.tasklet(name="steps_producer")
class StepsProducer:
    def run(self, value: int, ctx: flezha.Ctx[None, Any]) -> None:
        ctx.emit(value)


_DELIVERED: list[Any] = []
"""What a consumer of the ``handoff`` channel was handed, in delivery order."""


@flezha.tasklet(name="steps_b")
class StepsB:
    def run(self, value: int, ctx: flezha.Ctx[None, None]) -> None:
        _DELIVERED.append(value)


@flezha.tasklet(name="steps_c")
class StepsC:
    def run(self, value: int, ctx: flezha.Ctx[None, None]) -> None:
        _DELIVERED.append(value)


def _steps_graph(c: flezha.ChannelBuilder) -> None:
    edge = c.input("in").into(StepsProducer).named("handoff")
    edge.map(lambda value: value + 1).into(StepsB)
    edge.into(StepsC)
    edge.output()


@flezha.relay("steps_emit", run="steps")
def _steps_emit(c: flezha.ChannelBuilder) -> None:
    _steps_graph(c)


@flezha.relay("steps_emit_stream")
def _steps_emit_stream(c: flezha.ChannelBuilder) -> None:
    _steps_graph(c)


def _delivered(
    factory: str, tasklet: int, deps: DispatchDeps, *items: tuple[bytes, str]
) -> list[Any]:
    """What the ``handoff`` consumer is handed for ``(payload, form)`` items."""
    _DELIVERED.clear()
    activation = wire_work(50, factory, *[(1, payload) for payload, _ in items], tasklet=tasklet)
    for item, (_, form) in zip(activation["items"], items, strict=True):
        item["form"] = form
    for effect in answered(activation, deps)["effects"]:
        assert effect["verdict"] == "ok"
    return list(_DELIVERED)


def _payload_of(output: dict[str, Any]) -> Any:
    """An edge payload is the class codec's MessagePack; the ``handoff`` sink
    names no codec, so its frame for a number is JSON."""
    if output["target"] == "plugin":
        return json.loads(output["payload"])
    return codec.decode(output["payload"])


def test_a_structured_steps_releases_one_untransformed_edge_output(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_steps_emit, fake_core)
    (effect,) = answered(work_activation(40, 10, factory="steps_emit"), deps)["effects"]
    assert [
        (o["target"], o["consumer"], o["plugin_channel"], _payload_of(o)) for o in effect["outputs"]
    ] == [("edge", 1, None, 10), ("plugin", None, "handoff", 10)]
    assert effect["outputs"][0]["key"] is None
    assert effect["outputs"][0]["event_time"] is None


@flezha.relay("steps_carry", run={"steps": {"each": "chained"}})
def _steps_carry(c: flezha.ChannelBuilder) -> None:
    c.input("in").into(StepsProducer).named("carry")


def test_a_steps_terminal_nothing_consumes_hands_its_value_to_the_core_as_the_carry(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_steps_carry, fake_core)
    (effect,) = answered(work_activation(42, 3, factory="steps_carry"), deps)["effects"]
    assert [
        (o["target"], o["channel"], o["consumer"], _payload_of(o)) for o in effect["outputs"]
    ] == [("edge", 1, 0, 3)]


@flezha.relay("steps_call_end", run={"steps": {"each": "per_input"}})
def _steps_call_end(c: flezha.ChannelBuilder) -> None:
    asked = c.input("in").into(StepsProducer).named("ask")
    asked.call("pricing", deadline_ms=1_000).map(lambda response: response.value).into(StepsC)


def test_a_steps_terminal_ending_in_a_call_hands_the_core_no_edge_output(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_steps_call_end, fake_core)
    (effect,) = answered(work_activation(44, 5, factory="steps_call_end"), deps)["effects"]
    assert [(o["target"], o["plugin_channel"]) for o in effect["outputs"]] == [("plugin", "ask")]


def test_an_edge_payload_is_the_encoders_own_bytes(
    fake_core: FakeCore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MessagePack hands back finished bytes, so nothing is layered on them.

    The requirement this arms says a host whose encoder returns the bytes
    already satisfies it by doing nothing; a copy or a conversion introduced
    here would be exactly what it forbids.
    """
    produced: list[bytes] = []
    encode = codec.encode

    def recording(value: Any) -> bytes:
        made = encode(value)
        produced.append(made)
        return made

    monkeypatch.setattr(codec, "encode", recording)
    deps = deps_for(_steps_emit_stream, fake_core)
    (effect,) = answered(work_activation(43, 10, factory="steps_emit_stream"), deps)["effects"]

    assert len(effect["outputs"]) == 3
    for output in (o for o in effect["outputs"] if o["target"] == "edge"):
        assert any(made is output["payload"] for made in produced), (
            "the frame carries a copy of the bytes MessagePack returned, not the bytes"
        )


def test_the_same_graph_on_a_flow_class_releases_one_output_per_consumer(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_steps_emit_stream, fake_core)
    (effect,) = answered(work_activation(41, 10, factory="steps_emit_stream"), deps)["effects"]
    assert [(o["target"], o["consumer"], _payload_of(o)) for o in effect["outputs"]] == [
        ("edge", 1, 11),
        ("edge", 2, 10),
        ("plugin", None, 10),
    ]


# The other end of the same two arms: what the CONSUMER is handed. A stream
# producer already applied the edge, so delivery applies nothing; a steps relay
# stage released the value raw, so delivery is the one application.


def test_a_flow_inter_task_edge_applies_its_map_exactly_once(fake_core: FakeCore) -> None:
    deps = deps_for(_steps_emit_stream, fake_core)
    (effect,) = answered(work_activation(42, 10, factory="steps_emit_stream"), deps)["effects"]
    released = effect["outputs"][0]["payload"]
    assert _delivered("steps_emit_stream", 1, deps, (released, "transformed")) == [11]


def test_the_input_form_comes_from_the_core(fake_core: FakeCore) -> None:
    """The consumer applies its edge to what the core marks untransformed only,
    whatever the relay's run: an entry payload and a steps release arrive so,
    a stream producer's output arrives transformed."""
    deps = deps_for(_steps_emit_stream, fake_core)
    value = codec.encode(10)
    assert _delivered(
        "steps_emit_stream", 1, deps, (value, "untransformed"), (value, "transformed")
    ) == [11, 10]


def test_a_steps_consumer_applies_its_own_edge_exactly_once_at_delivery(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_steps_emit, fake_core)
    (effect,) = answered(work_activation(43, 10, factory="steps_emit"), deps)["effects"]
    released = effect["outputs"][0]["payload"]
    # One released value reaches BOTH children — the driver's fan-out — and
    # each evaluates its own edge here.
    assert _delivered("steps_emit", 1, deps, (released, "untransformed")) == [11]
    assert _delivered("steps_emit", 2, deps, (released, "untransformed")) == [10]


def test_plain_text_into_an_entry_naming_no_codec_never_reaches_the_handler(
    fake_core: FakeCore,
) -> None:
    """An entry naming no codec reads JSON, so plain text fails its decode: the
    verdict says the message is unreadable (the core parks it under the entry
    channel, and a steps keeps waiting), and the handler is never handed it."""
    seen: list[Any] = []

    @flezha.tasklet(name="record")
    class Record:
        def run(self, value: Any, ctx: flezha.Ctx[None, None]) -> None:
            seen.append(value)

    activation = wire_work(60, "solo", (0, b'{"a": 1}'), (0, b"hello"))
    effects = answered(activation, solo_deps(Record, fake_core))["effects"]
    assert [effect["verdict"] for effect in effects] == ["ok", "unreadable"]
    assert effects[1]["message"].startswith("JSONDecodeError")
    assert seen == [{"a": 1}]


def test_an_entry_edge_transform_that_raises_makes_the_message_unreadable(
    fake_core: FakeCore,
) -> None:
    """The entry edge's transforms run at the entrance too, so a mapping that
    rejects a message parks it as an unreadable one rather than failing the
    handler that never saw it."""
    seen: list[Any] = []

    @flezha.tasklet(name="record")
    class Record:
        def run(self, value: Any, ctx: flezha.Ctx[None, None]) -> None:
            seen.append(value)

    def strict(value: Any) -> Any:
        if "k" not in value:
            raise KeyError("k")
        return value["k"]

    @flezha.relay("strict_entry")
    def strict_entry(c: flezha.ChannelBuilder) -> None:
        c.input("in").map(strict).into(Record).output()

    deps = deps_for(strict_entry, fake_core)
    effects = answered(
        wire_work(63, "strict_entry", (0, b'{"k": "fine"}'), (0, b'{"other": 1}')), deps
    )["effects"]
    assert [effect["verdict"] for effect in effects] == ["ok", "unreadable"]
    assert effects[1]["message"].startswith("KeyError")
    assert seen == ["fine"]


def test_an_unreadable_message_is_told_apart_from_a_handler_failure(
    fake_core: FakeCore,
) -> None:
    """Only the entrance answers `unreadable`: a readable message the handler
    raises on is the handler's failure, retryable as before."""

    @flezha.tasklet(name="refuse")
    class Refuse:
        def run(self, value: Any, ctx: flezha.Ctx[None, None]) -> None:
            raise ValueError("cannot place this")

    deps = solo_deps(Refuse, fake_core)
    (unreadable,) = answered(wire_work(61, "solo", (0, b"not json")), deps)["effects"]
    assert unreadable["verdict"] == "unreadable"
    assert unreadable["message"].startswith("JSONDecodeError")
    (failed,) = answered(wire_work(62, "solo", (0, b'{"a": 1}')), deps)["effects"]
    assert (failed["verdict"], failed["message"]) == ("retryable", "ValueError: cannot place this")


# -- merge and flat_map ----------------------------------------------------------
#
# Driven over the test app's own `merged_input` and `transform_chain`. Both output
# keyed, so a work activation names the cell it is handed to.


def keys_answered(answer: dict[str, Any]) -> list[str]:
    """Every key a prepare answer's values name, in answer order."""
    return [key for value in answer["values"] for key in value["keys"]]


def _plugin_payloads(result: dict[str, Any]) -> list[bytes]:
    return [
        output["payload"]
        for effect in result["effects"]
        for output in effect["outputs"]
        if output["target"] == "plugin"
    ]


def test_interleaved_merged_sources_each_reach_the_tasklet_under_their_own_mapping(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(fixtures.merged_input_relay, fake_core)
    activation = wire_work(
        70, "merged_input", (0, b"MSFT:1"), (1, b"MSFT:2"), (0, b"MSFT:3"), cell="MSFT"
    )
    assert _plugin_payloads(answered(activation, deps)) == [
        b"ticks:MSFT:1",
        b"quotes:MSFT:2",
        b"ticks:MSFT:3",
    ]


def test_a_merged_entry_source_is_keyed_on_its_mapped_value(fake_core: FakeCore) -> None:
    deps = deps_for(fixtures.merged_input_relay, fake_core)
    for channel in (0, 1):
        result = answered(
            key_request(71, "merged_input", b"AAPL:1", b"MSFT", channel=channel), deps
        )
        assert [keys_answered(answer) for answer in result["prepared"]] == [["AAPL"], ["MSFT"]]


@flezha.relay("merged_edge")
def _merged_edge(c: flezha.ChannelBuilder) -> None:
    ticks = c.input("ticks", codec="text")
    quotes = c.input("quotes", codec="text").into(echo)
    c.merge().from_(ticks, fixtures.tagged("ticks")).from_(
        quotes, fixtures.tagged("quotes")
    ).key_by(lambda value: value["symbol"]).into(fixtures.MergeLabel).output("merged")


def test_an_inter_tasklet_merge_source_is_mapped_and_keyed_as_it_is_released(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_merged_edge, fake_core)
    (effect,) = answered(wire_work(72, "merged_edge", (1, b"AAPL:2")), deps)["effects"]
    (released,) = effect["outputs"]
    assert (released["consumer"], released["key"]) == (1, "AAPL")
    mapped = {"source": "quotes", "symbol": "AAPL", "payload": "AAPL:2"}
    assert codec.decode(released["payload"]) == mapped

    # Delivery applies nothing more: the producer already ran the edge.
    activation = wire_work(
        73, "merged_edge", (0, b"AAPL:1"), (2, released["payload"]), tasklet=1, cell="AAPL"
    )
    activation["items"][1]["form"] = "transformed"
    assert _plugin_payloads(answered(activation, deps)) == [b"ticks:AAPL:1", b"quotes:AAPL:2"]


def test_a_fan_of_three_runs_the_handler_three_times_under_one_effect(fake_core: FakeCore) -> None:
    deps = deps_for(fixtures.transform_chain_relay, fake_core)
    result = answered(wire_work(80, "transform_chain", (0, b"a:1,a:2,a:3"), cell="A"), deps)
    (effect,) = result["effects"]
    assert (effect["verdict"], effect["consumed_offset"]) == ("ok", 80)
    assert _plugin_payloads(result) == [b"A:1", b"A:2", b"A:3"]


def test_a_fan_then_key_by_answers_every_value_with_its_key_at_the_crossing(
    fake_core: FakeCore,
) -> None:
    activation = key_request(81, "transform_chain", b"a:1,b:2,c:3", b"a:1,a:2", b"drop")
    result = answered(activation, deps_for(fixtures.transform_chain_relay, fake_core))
    assert [keys_answered(answer) for answer in result["prepared"]] == [
        ["A", "B", "C"],
        ["A", "A"],
        [],
    ]


@flezha.relay("decoded_once")
def _decoded_once(c: flezha.ChannelBuilder) -> None:
    c.input("trades", decode=_counted_decode).flat_map(lambda trade: trade.split(",")).key_by(
        fixture_support.prefix
    ).into(fixtures.transform_label).keyed().output("transformed")


_DECODED: list[bytes] = []


def _counted_decode(raw: bytes) -> str:
    _DECODED.append(raw)
    return raw.decode()


def test_a_keyed_entry_payload_is_decoded_and_transformed_once(fake_core: FakeCore) -> None:
    """The crossing decodes the payload and runs the chain once; each Cell runs
    its own prepared values with neither the entry decoder nor the edge."""
    _DECODED.clear()
    deps = deps_for(_decoded_once, fake_core)
    (answer,) = answered(key_request(87, "decoded_once", b"a:1,a:2,b:3"), deps)["prepared"]
    by_cell: dict[str, list[bytes]] = {}
    for value in answer["values"]:
        (key,) = value["keys"]
        by_cell.setdefault(key, []).append(value["bytes"])

    ran = {}
    for cell, values in by_cell.items():
        activation = wire_work(88, "decoded_once", (0, b"a:1,a:2,b:3"), cell=cell)
        activation["items"][0].update(form="prepared", values=values)
        ran[cell] = _plugin_payloads(answered(activation, deps))
    assert ran == {"a": [b"a:1", b"a:2"], "b": [b"b:3"]}
    assert _DECODED == [b"a:1,a:2,b:3"]


@flezha.relay("fanned_static")
def _fanned_static(c: flezha.ChannelBuilder) -> None:
    c.input("in", codec="text").flat_map(lambda line: line.split(",")).into(
        fixtures.static_entry
    ).output()


def test_a_fan_into_a_static_consumer_answers_each_value_with_the_cells_its_router_names(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_fanned_static, fake_core)
    crossing = key_request(85, "fanned_static", b"a:1,b:2,x:3")
    (answer,) = answered(crossing, deps)["prepared"]
    assert [(value["keys"], codec.decode(value["bytes"])) for value in answer["values"]] == [
        (["a"], "a:1"),
        (["b"], "b:2"),
        (["d"], "x:3"),
    ]


@flezha.relay("fanned_edge")
def _fanned_edge(c: flezha.ChannelBuilder) -> None:
    c.input("in", codec="text").into(echo).flat_map(
        lambda line: [part for part in line.split(",") if part]
    ).key_by(fixture_support.prefix).into(fixtures.transform_label)


def test_a_producer_side_fan_releases_one_keyed_frame_per_value_and_none_for_an_empty_fan(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_fanned_edge, fake_core)
    (effect,) = answered(wire_work(83, "fanned_edge", (0, b"a:1,b:2")), deps)["effects"]
    assert [(o["consumer"], o["key"], codec.decode(o["payload"])) for o in effect["outputs"]] == [
        (1, "a", "a:1"),
        (1, "b", "b:2"),
    ]
    (effect,) = answered(wire_work(84, "fanned_edge", (0, b"")), deps)["effects"]
    assert (effect["verdict"], effect["outputs"]) == ("ok", [])


# -- tasklet-requested spawn -------------------------------------------------


def test_a_spawn_returns_before_the_controller_answers_and_its_refusal_stays_in_the_handle(
    fake_core: FakeCore,
) -> None:
    """The call never waits for the controller and never raises its refusal
    into the tasklet: the handle is pending until the answer arrives, and a
    read is what raises it."""
    fake_core.spawn_answer_after_s = 0.5
    fake_core.spawn_answer = flezha.FlezhaError(
        "quota.exceeded", {"kind": "max_relays", "current": 3, "limit": 3}
    )
    seen: dict[str, Any] = {}

    @flezha.tasklet(name="spawner")
    class Spawner:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            began = time.monotonic()
            handle = ctx.spawn_steps("worker", value)
            seen["returned_after_s"] = time.monotonic() - began
            seen["pending"] = handle.try_result()
            try:
                handle.wait()
            except flezha.FlezhaError as refusal:
                seen["refusal"] = refusal
            ctx.emit(value)

    (effect,) = answered(work_activation(90, "p", factory="solo"), solo_deps(Spawner, fake_core))[
        "effects"
    ]
    assert seen["returned_after_s"] < fake_core.spawn_answer_after_s / 5
    assert seen["pending"] is None
    assert (seen["refusal"].code, seen["refusal"].details["kind"]) == (
        "quota.exceeded",
        "max_relays",
    )
    assert (effect["verdict"], effect["outputs"][0]["payload"]) == ("ok", b"p")


def test_a_spawn_without_an_id_asks_for_one_and_a_spawn_with_an_id_carries_it(
    fake_core: FakeCore,
) -> None:
    results: list[flezha.SpawnResult] = []

    @flezha.tasklet(name="spawner")
    class Spawner:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            results.append(ctx.spawn_steps("worker", value).wait())
            results.append(ctx.spawn_steps("worker", value, id="w1").wait())

    answered(work_activation(91, "p", factory="solo"), solo_deps(Spawner, fake_core))
    assert results == [
        flezha.SpawnResult("worker-k7m2qzxw4pd3fn2a", "spawned"),
        flezha.SpawnResult("w1", "spawned"),
    ]
    assert [
        (
            s["token"],
            s["factory_id"],
            s["id"],
            s["payload"],
            s["if_running"],
            s["replay"],
            s["offset"],
        )
        for s in fake_core.spawns
    ] == [
        (91, "worker", "", b"p", "", False, 91),
        (91, "worker", "w1", b"p", "", False, 91),
    ]


def test_a_spawn_payload_is_what_a_text_or_json_entry_reads(fake_core: FakeCore) -> None:
    """The payload takes the no-codec sink rule `complete` applies: a string is
    its UTF-8, bytes are themselves, anything else its JSON, so the spawned
    steps's `text` or `json` entry decodes it as a send's payload."""

    @flezha.tasklet(name="spawner")
    class Spawner:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            ctx.spawn_steps("place", "o-1").wait()
            ctx.spawn_steps("place", b"\xff\x00raw").wait()
            ctx.spawn_steps("place", {"symbol": "AAPL", "quantity": 10}).wait()

    answered(work_activation(92, "p", factory="solo"), solo_deps(Spawner, fake_core))
    payloads = [s["payload"] for s in fake_core.spawns]
    assert payloads[0] == b"o-1"
    assert payloads[1] == b"\xff\x00raw"
    assert json.loads(payloads[2]) == {"symbol": "AAPL", "quantity": 10}


@pytest.mark.parametrize(
    ("code", "details"),
    [
        ("relay.already_running", {"relay": "w1"}),
        ("relay.factory_unknown", {"namespace": "default", "factory": "worker"}),
        ("relay.factory_not_steps", {"namespace": "default", "factory": "worker"}),
        ("authz.no_role_binding", {}),
        ("spawn.timeout", {"seconds": 30}),
        ("spawn.unavailable", {}),
    ],
)
def test_each_failure_a_read_meets_is_the_one_error_with_its_code(
    fake_core: FakeCore, code: str, details: dict[str, Any]
) -> None:
    fake_core.spawn_answer = flezha.FlezhaError(code, details)
    raised: list[flezha.FlezhaError] = []

    @flezha.tasklet(name="spawner")
    class Spawner:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            handle = ctx.spawn_steps("worker", value, id="w1", if_running="error")
            try:
                handle.wait()
            except flezha.FlezhaError as error:
                raised.append(error)

    (effect,) = answered(work_activation(92, "p", factory="solo"), solo_deps(Spawner, fake_core))[
        "effects"
    ]
    (error,) = raised
    assert (error.code, error.details) == (code, details)
    assert fake_core.spawns[0]["if_running"] == "error"
    assert effect["verdict"] == "ok"


def test_if_running_without_an_id_raises_the_cores_refusal_and_sends_nothing(
    fake_core: FakeCore,
) -> None:
    raised: list[flezha.FlezhaError] = []

    @flezha.tasklet(name="spawner")
    class Spawner:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            try:
                ctx.spawn_steps("worker", value, if_running="error")
            except flezha.FlezhaError as error:
                raised.append(error)

    answered(work_activation(93, "p", factory="solo"), solo_deps(Spawner, fake_core))
    (error,) = raised
    assert error.code == "spawn.if_running_without_id"
    assert fake_core.spawns == []


def test_a_replayed_spawn_hands_the_replay_to_the_core_and_answers_the_id_it_was_given(
    fake_core: FakeCore,
) -> None:
    """The core decides a replay sends nothing (asserted in its own suite); what
    the SDK owns is handing it the flag and reading the answer."""
    results: list[flezha.SpawnResult | None] = []

    @flezha.tasklet(name="spawner")
    class Spawner:
        def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            results.append(ctx.spawn_steps("worker", value).try_result())
            results.append(ctx.spawn_steps("worker", value, id="w1").try_result())

    activation = work_activation(94, "p", factory="solo")
    activation["items"][0]["replay"] = True
    answered(activation, solo_deps(Spawner, fake_core))
    assert results == [
        flezha.SpawnResult("", "skipped_on_replay"),
        flezha.SpawnResult("w1", "skipped_on_replay"),
    ]
    assert [spawn["replay"] for spawn in fake_core.spawns] == [True, True]


def test_an_async_handler_awaits_a_spawn_while_its_loop_keeps_serving(fake_core: FakeCore) -> None:
    fake_core.spawn_answer_after_s = 0.3
    results: list[flezha.SpawnResult] = []

    @flezha.tasklet(name="spawner")
    class Spawner:
        async def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
            results.append(await ctx.spawn_steps("worker", value, id="w1"))

    deps = solo_deps(Spawner, fake_core)

    async def serve() -> tuple[int, dict[str, Any]]:
        ticks = 0
        answering = asyncio.ensure_future(answer(work_activation(95, "p", factory="solo"), deps))
        while not answering.done():
            await asyncio.sleep(0.01)
            ticks += 1
        return ticks, await answering

    ticks, result = asyncio.run(serve())
    assert ticks >= 10, "the loop stalled while the handler waited for the spawn"
    assert [effect["verdict"] for effect in result["effects"]] == ["ok"]
    assert results == [flezha.SpawnResult("w1", "spawned")]
