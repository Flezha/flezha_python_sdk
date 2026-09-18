"""One activation in, one answer out — and never none.

``answer`` is a pure function of the activation and the registry, so every
mapping below asserts what it returns. The handout contract — an activation
that returns unanswered retires its entity for the life of the process — is
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
from conftest import FakeCore, compensating, demo, deps_for, echo, solo_deps, work_activation
from flezha import codec, native
from flezha.dispatch import DispatchDeps, answer, dispatch_now, run_now
from flezha.errors import NonRetryableError

fixtures = app.module("fixtures")
fixture_support = app.module("fixture_support")


def answered(activation: dict[str, Any], deps: DispatchDeps) -> dict[str, Any]:
    """The answer for one activation, driven with no event loop."""
    return run_now(answer(activation, deps))


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
    @flezha.procedure
    def boom(value: str) -> str:
        raise ValueError("nope")

    deps = solo_deps(boom, fake_core)
    (effect,) = answered(work_activation(3, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "retryable"
    assert effect["message"] == "ValueError: nope"


def test_a_non_retryable_error_says_so(fake_core: FakeCore) -> None:
    @flezha.procedure
    def refuse(value: str) -> str:
        raise NonRetryableError("bad input")

    deps = solo_deps(refuse, fake_core)
    (effect,) = answered(work_activation(4, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "non_retryable"


def test_a_batch_stops_at_the_first_failure(fake_core: FakeCore) -> None:
    seen: list[str] = []

    @flezha.procedure
    def picky(value: str) -> str:
        seen.append(value)
        if value == "bad":
            raise ValueError("nope")
        return value

    deps = solo_deps(picky, fake_core)
    effects = answered(work_activation(5, "ok", "bad", "never", factory="solo"), deps)["effects"]
    # Items after the failure are re-handed by the core, so the batch stops
    # rather than reporting effects the entity never saw.
    assert seen == ["ok", "bad"]
    assert [effect["verdict"] for effect in effects] == ["ok", "retryable"]


def test_a_failure_inside_complete_abandons_rather_than_retiring_the_entity(
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
    assert "unknown activation token 6" in caplog.text
    assert "abandoning activation 6 failed" in caplog.text


def test_an_async_handler_is_driven_off_an_event_loop(fake_core: FakeCore) -> None:
    @flezha.procedure
    async def slow(value: str) -> str:
        await asyncio.sleep(0)
        return value.upper()

    deps = solo_deps(slow, fake_core)
    (effect,) = answered(work_activation(7, "hi", factory="solo"), deps)["effects"]
    assert effect["outputs"][0]["payload"] == b"HI"


def test_a_filtered_input_is_consumed_and_reaches_no_handler(fake_core: FakeCore) -> None:
    seen: list[str] = []

    @flezha.procedure
    def only_long(value: str) -> str:
        seen.append(value)
        return value

    @flezha.stream_operative("filtered")
    def filtered(c: flezha.Channels) -> None:
        c.input("in").filter(lambda value: len(value) > 2).into(only_long).sink()

    deps = deps_for(filtered, fake_core)

    (effect,) = answered(work_activation(8, "ab", factory="filtered"), deps)["effects"]
    assert seen == []
    # The offset still advances: the item is consumed, not retried.
    assert effect["verdict"] == "ok"
    assert effect["consumed_offset"] == 8
    assert effect["outputs"] == []


def key_request(token: int, factory: str, *payloads: bytes, channel: int = 0) -> dict[str, Any]:
    """One key request over wire payloads, as the veneer produces it."""
    return {
        "token": token,
        "operative": "op",
        "procedure": 0,
        "change_seq": 0,
        "factory": factory,
        "kind": "key_request",
        "key_request": {
            "channel": channel,
            "payloads": list(payloads),
            "want_keys": True,
            "want_event_time": False,
        },
    }


def wire_work(
    token: int,
    factory: str,
    *items: tuple[int, bytes],
    procedure: int = 0,
    entity: str | None = None,
) -> dict[str, Any]:
    """One work activation over wire payloads, ``(channel, payload)`` per item."""
    activation = work_activation(token, *[None] * len(items), factory=factory, procedure=procedure)
    activation["entity_key"] = entity
    for item, (channel, payload) in zip(activation["items"], items, strict=True):
        item.update(channel=channel, payload=payload)
    return activation


def test_a_key_request_answers_one_entry_per_payload(fake_core: FakeCore) -> None:
    @flezha.procedure(dynamic_entities=True)
    def routed(value: dict[str, str]) -> dict[str, str]:
        return value

    @flezha.stream_operative("routed_op")
    def routed_op(c: flezha.Channels) -> None:
        c.input("in").key_by(lambda value: value["k"]).into(routed).sink()

    deps = deps_for(routed_op, fake_core)

    result = answered(key_request(9, "routed_op", b'{"k": "a"}', b'{"k": "b"}'), deps)
    assert result["outcome"] == "prepared"
    assert [answer["keys"] for answer in result["prepared"]] == [["a"], ["b"]]


def test_a_per_payload_refusal_does_not_fail_the_whole_crossing(fake_core: FakeCore) -> None:
    @flezha.procedure(dynamic_entities=True)
    def routed(value: dict[str, str]) -> dict[str, str]:
        return value

    def key_of(value: dict[str, str]) -> str:
        if value["k"] == "bad":
            raise ValueError("no key")
        return value["k"]

    @flezha.stream_operative("picky_keys")
    def picky_keys(c: flezha.Channels) -> None:
        c.input("in").key_by(key_of).into(routed).sink()

    deps = deps_for(picky_keys, fake_core)

    crossing = key_request(10, "picky_keys", b'{"k": "bad"}', b'{"k": "ok"}')
    answers = answered(crossing, deps)["prepared"]
    assert "no key" in answers[0]["error"]
    assert answers[1]["keys"] == ["ok"]


def test_a_live_state_read_answers_json(deps: DispatchDeps) -> None:
    # Straight to the stateful procedure: an answer routes no edges, so an
    # input to `echo` would never reach `count_up`.
    answered(work_activation(11, "abc", procedure=1), deps)
    result = answered(
        {
            "token": 12,
            "operative": "op",
            "procedure": 1,
            "change_seq": 0,
            "factory": "demo",
            "kind": "live_state_read",
        },
        deps,
    )
    assert result["outcome"] == "state_json"
    assert codec.from_state_json(result["json"]) == {"count": 3, "seen": ["3"]}


def test_a_snapshot_and_a_restore_fold_round_trip(deps: DispatchDeps) -> None:
    answered(work_activation(13, "abcd", procedure=1), deps)
    snapshot = answered(
        {
            "token": 14,
            "operative": "op",
            "procedure": 1,
            "change_seq": 0,
            "factory": "demo",
            "kind": "snapshot_request",
        },
        deps,
    )["state"]

    restored = answered(
        {
            "token": 15,
            "operative": "op",
            "procedure": 1,
            "entity_key": "other",
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
    @flezha.procedure
    def stamped(value: str, ctx: flezha.Ctx[None]) -> None:
        ctx.emit(value, metadata={"flz.seq": "1"})

    deps = solo_deps(stamped, fake_core)
    (effect,) = answered(work_activation(17, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "retryable"
    assert "flz.seq" in effect["message"]


def test_a_keyed_channel_with_no_key_is_refused_naming_it(fake_core: FakeCore) -> None:
    @flezha.procedure
    def keyless(value: str, ctx: flezha.Ctx[None]) -> None:
        ctx.emit(value)

    @flezha.stream_operative("keyed_sink")
    def keyed_sink(c: flezha.Channels) -> None:
        c.input("in").into(keyless).keyed().sink("out")

    deps = deps_for(keyed_sink, fake_core)
    (effect,) = answered(work_activation(18, "x", factory="keyed_sink"), deps)["effects"]
    assert "out" in effect["message"]


# -- a procedure ending its workflow -------------------------------------------
#
# The host only carries the request in the item's effect. Ending the instance
# after the iteration commits, and failing a stream's item, are the core's.

_RESULTS: dict[str, Any] = {"text": "3", "bytes": b"\x00raw", "json": {"turns": 3}, "none": None}


@flezha.procedure(name="finisher")
def _finisher(value: str, ctx: flezha.Ctx[None]) -> None:
    ctx.complete("replaced")
    ctx.complete(_RESULTS[value])


@flezha.workflow_operative("finishing_workflow")
def _finishing_workflow(c: flezha.Channels) -> None:
    c.input("in").into(_finisher).sink()


@flezha.stream_operative("finishing_stream")
def _finishing_stream(c: flezha.Channels) -> None:
    c.input("in").into(_finisher).sink()


@pytest.mark.parametrize(
    ("value", "carried"),
    [("text", b"3"), ("bytes", b"\x00raw"), ("json", b'{"turns":3}'), ("none", b"")],
)
def test_complete_carries_the_latest_result_in_the_effect(
    fake_core: FakeCore, value: str, carried: bytes
) -> None:
    deps = deps_for(_finishing_workflow, fake_core)
    (effect,) = answered(work_activation(19, value, factory="finishing_workflow"), deps)["effects"]
    assert effect["verdict"] == "ok"
    assert effect["complete"] == carried


def test_a_stream_procedure_completing_is_carried_the_same_way(fake_core: FakeCore) -> None:
    deps = deps_for(_finishing_stream, fake_core)
    (effect,) = answered(work_activation(20, "text", factory="finishing_stream"), deps)["effects"]
    assert effect["complete"] == b"3"


def test_a_failed_item_carries_no_completion(fake_core: FakeCore) -> None:
    @flezha.procedure
    def fails_after_completing(value: str, ctx: flezha.Ctx[None]) -> None:
        ctx.complete(value)
        raise ValueError("after completing")

    deps = solo_deps(fails_after_completing, fake_core)
    (effect,) = answered(work_activation(21, "x", factory="solo"), deps)["effects"]
    assert effect["verdict"] == "retryable"
    assert "complete" not in effect


def test_a_compensation_runs_over_the_recorded_outputs(fake_core: FakeCore) -> None:
    seen: list[Any] = []
    deps = solo_deps(compensating(seen), fake_core)
    result = answered(
        {
            "token": 20,
            "operative": "op",
            "procedure": 0,
            "change_seq": 0,
            "factory": "solo",
            "kind": "compensate_run",
            "outputs": [codec.encode("a"), codec.encode("b")],
        },
        deps,
    )
    assert seen == ["a", "b"]
    assert result["outcome"] == "work"


# -- the structured-workflow emit arm ------------------------------------------
#
# One released value is ONE edge output on a workflow class, untransformed and
# unkeyed, naming the producing channel and its first consumer: the driver fans
# it out from the journaled handoff. On a stream class the same topology
# releases one output per consumer, transformed and keyed. The plugin output
# rides alongside either way.


@flezha.procedure(name="wf_producer")
def _wf_producer(value: int, ctx: flezha.Ctx[None]) -> None:
    ctx.emit(value)


_DELIVERED: list[Any] = []
"""What a consumer of the ``handoff`` channel was handed, in delivery order."""


@flezha.procedure(name="wf_b")
def _wf_b(value: int) -> None:
    _DELIVERED.append(value)


@flezha.procedure(name="wf_c")
def _wf_c(value: int) -> None:
    _DELIVERED.append(value)


def _workflow_graph(c: flezha.Channels) -> None:
    edge = c.input("in").into(_wf_producer).named("handoff")
    edge.map(lambda value: value + 1).into(_wf_b)
    edge.into(_wf_c)
    edge.sink()


@flezha.workflow_operative("wf_emit")
def _wf_emit(c: flezha.Channels) -> None:
    _workflow_graph(c)


@flezha.stream_operative("wf_emit_stream")
def _wf_emit_stream(c: flezha.Channels) -> None:
    _workflow_graph(c)


def _delivered(factory: str, procedure: int, payload: bytes, deps: DispatchDeps) -> list[Any]:
    _DELIVERED.clear()
    (effect,) = answered(wire_work(50, factory, (1, payload), procedure=procedure), deps)["effects"]
    assert effect["verdict"] == "ok"
    return list(_DELIVERED)


def _payload_of(output: dict[str, Any]) -> Any:
    """An edge payload is the class codec's MessagePack; the ``handoff`` sink
    names no codec, so its frame for a number is JSON."""
    if output["target"] == "plugin":
        return json.loads(output["payload"])
    return codec.decode(output["payload"])


def test_a_structured_workflow_releases_one_untransformed_edge_output(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_wf_emit, fake_core)
    (effect,) = answered(work_activation(40, 10, factory="wf_emit"), deps)["effects"]
    assert [
        (o["target"], o["consumer"], o["plugin_channel"], _payload_of(o)) for o in effect["outputs"]
    ] == [("edge", 1, None, 10), ("plugin", None, "handoff", 10)]
    assert effect["outputs"][0]["key"] is None
    assert effect["outputs"][0]["event_time"] is None


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
    deps = deps_for(_wf_emit_stream, fake_core)
    (effect,) = answered(work_activation(43, 10, factory="wf_emit_stream"), deps)["effects"]

    assert len(effect["outputs"]) == 3
    for output in (o for o in effect["outputs"] if o["target"] == "edge"):
        assert any(made is output["payload"] for made in produced), (
            "the frame carries a copy of the bytes MessagePack returned, not the bytes"
        )


def test_the_same_graph_on_a_stream_class_releases_one_output_per_consumer(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_wf_emit_stream, fake_core)
    (effect,) = answered(work_activation(41, 10, factory="wf_emit_stream"), deps)["effects"]
    assert [(o["target"], o["consumer"], _payload_of(o)) for o in effect["outputs"]] == [
        ("edge", 1, 11),
        ("edge", 2, 10),
        ("plugin", None, 10),
    ]


# The other end of the same two arms: what the CONSUMER is handed. A stream
# producer already applied the edge, so delivery applies nothing; a workflow
# stage released the value raw, so delivery is the one application.


def test_a_stream_inter_proc_edge_applies_its_map_exactly_once(fake_core: FakeCore) -> None:
    deps = deps_for(_wf_emit_stream, fake_core)
    (effect,) = answered(work_activation(42, 10, factory="wf_emit_stream"), deps)["effects"]
    assert _delivered("wf_emit_stream", 1, effect["outputs"][0]["payload"], deps) == [11]


def test_a_workflow_consumer_applies_its_own_edge_exactly_once_at_delivery(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_wf_emit, fake_core)
    (effect,) = answered(work_activation(43, 10, factory="wf_emit"), deps)["effects"]
    released = effect["outputs"][0]["payload"]
    # One released value reaches BOTH children — the driver's fan-out — and
    # each evaluates its own edge here.
    assert _delivered("wf_emit", 1, released, deps) == [11]
    assert _delivered("wf_emit", 2, released, deps) == [10]


def test_plain_text_into_an_entry_naming_no_codec_never_reaches_the_handler(
    fake_core: FakeCore,
) -> None:
    """An entry naming no codec reads JSON, so plain text fails its decode: the
    item rides the retry ladder and the handler is never handed it."""
    seen: list[Any] = []

    @flezha.procedure
    def record(value: Any) -> None:
        seen.append(value)

    activation = wire_work(60, "solo", (0, b'{"a": 1}'), (0, b"hello"))
    effects = answered(activation, solo_deps(record, fake_core))["effects"]
    assert [effect["verdict"] for effect in effects] == ["ok", "retryable"]
    assert effects[1]["message"].startswith("JSONDecodeError")
    assert seen == [{"a": 1}]


# -- merge and flat_map ----------------------------------------------------------
#
# Driven over the test app's own `merged_input` and `transform_chain`. Both sink
# keyed, so a work activation names the entity it is handed to.


def _plugin_payloads(result: dict[str, Any]) -> list[bytes]:
    return [
        output["payload"]
        for effect in result["effects"]
        for output in effect["outputs"]
        if output["target"] == "plugin"
    ]


def test_interleaved_merged_sources_each_reach_the_procedure_under_their_own_mapping(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(fixtures.merged_input_operative, fake_core)
    activation = wire_work(
        70, "merged_input", (0, b"MSFT:1"), (1, b"MSFT:2"), (0, b"MSFT:3"), entity="MSFT"
    )
    assert _plugin_payloads(answered(activation, deps)) == [
        b"ticks:MSFT:1",
        b"quotes:MSFT:2",
        b"ticks:MSFT:3",
    ]


def test_a_merged_entry_source_is_keyed_on_its_mapped_value(fake_core: FakeCore) -> None:
    deps = deps_for(fixtures.merged_input_operative, fake_core)
    for channel in (0, 1):
        result = answered(
            key_request(71, "merged_input", b"AAPL:1", b"MSFT", channel=channel), deps
        )
        assert [answer["keys"] for answer in result["prepared"]] == [["AAPL"], ["MSFT"]]


@flezha.stream_operative("merged_edge")
def _merged_edge(c: flezha.Channels) -> None:
    ticks = c.input("ticks", codec="text")
    quotes = c.input("quotes", codec="text").into(echo)
    c.merge().from_(ticks, fixtures.tagged("ticks")).from_(
        quotes, fixtures.tagged("quotes")
    ).key_by(lambda value: value["symbol"]).into(fixtures.merge_label).sink("merged")


def test_an_inter_procedure_merge_source_is_mapped_and_keyed_as_it_is_released(
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
        73, "merged_edge", (0, b"AAPL:1"), (2, released["payload"]), procedure=1, entity="AAPL"
    )
    assert _plugin_payloads(answered(activation, deps)) == [b"ticks:AAPL:1", b"quotes:AAPL:2"]


def test_a_fan_of_three_runs_the_handler_three_times_under_one_effect(fake_core: FakeCore) -> None:
    deps = deps_for(fixtures.transform_chain_operative, fake_core)
    result = answered(wire_work(80, "transform_chain", (0, b"a:1,a:2,a:3"), entity="A"), deps)
    (effect,) = result["effects"]
    assert (effect["verdict"], effect["consumed_offset"]) == ("ok", 80)
    assert _plugin_payloads(result) == [b"A:1", b"A:2", b"A:3"]


def test_a_fan_then_key_by_answers_every_key_once_at_the_crossing(fake_core: FakeCore) -> None:
    activation = key_request(81, "transform_chain", b"a:1,b:2,c:3", b"a:1,a:2", b"drop")
    result = answered(activation, deps_for(fixtures.transform_chain_operative, fake_core))
    assert [answer["keys"] for answer in result["prepared"]] == [["A", "B", "C"], ["A"], []]


def test_an_entity_handed_a_fanned_payload_runs_only_the_values_keyed_to_it(
    fake_core: FakeCore,
) -> None:
    """The core hands the raw payload to every key the crossing answered, and
    each entity re-applies the chain."""
    deps = deps_for(fixtures.transform_chain_operative, fake_core)
    result = answered(wire_work(82, "transform_chain", (0, b"a:1,b:2,c:3"), entity="B"), deps)
    assert _plugin_payloads(result) == [b"B:2"]


@flezha.stream_operative("fanned_static")
def _fanned_static(c: flezha.Channels) -> None:
    c.input("in", codec="text").flat_map(lambda line: line.split(",")).into(
        fixtures.static_entry
    ).sink()


def test_a_fan_into_a_static_consumer_runs_each_value_only_where_its_router_names(
    fake_core: FakeCore,
) -> None:
    deps = deps_for(_fanned_static, fake_core)
    crossing = key_request(85, "fanned_static", b"a:1,b:2,x:3")
    assert [answer["keys"] for answer in answered(crossing, deps)["prepared"]] == [["a", "b", "d"]]
    # `c` is named by no value, so it runs nothing even when handed the payload.
    assert {
        entity: _plugin_payloads(
            answered(wire_work(86, "fanned_static", (0, b"a:1,b:2,x:3"), entity=entity), deps)
        )
        for entity in ("a", "b", "c", "d")
    } == {"a": [b"a:1"], "b": [b"b:2"], "c": [], "d": [b"x:3"]}


@flezha.stream_operative("fanned_edge")
def _fanned_edge(c: flezha.Channels) -> None:
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


# -- procedure-requested spawn -------------------------------------------------


def test_a_spawn_returns_before_the_controller_answers_and_its_refusal_stays_in_the_handle(
    fake_core: FakeCore,
) -> None:
    """The call never waits for the controller and never raises its refusal
    into the procedure: the handle is pending until the answer arrives, and a
    read is what raises it."""
    fake_core.spawn_answer_after_s = 0.5
    fake_core.spawn_answer = {
        "error": "refused",
        "message": "the controller refused the spawn: quota_exceeded",
    }
    seen: dict[str, Any] = {}

    @flezha.procedure
    def spawner(value: str, ctx: flezha.Ctx[None]) -> str:
        began = time.monotonic()
        handle = ctx.spawn_workflow("worker", value)
        seen["returned_after_s"] = time.monotonic() - began
        seen["pending"] = handle.try_result()
        try:
            handle.wait()
        except flezha.SpawnRefusedError as refusal:
            seen["refusal"] = refusal
        return value

    (effect,) = answered(work_activation(90, "p", factory="solo"), solo_deps(spawner, fake_core))[
        "effects"
    ]
    assert seen["returned_after_s"] < fake_core.spawn_answer_after_s / 5
    assert seen["pending"] is None
    assert "quota_exceeded" in str(seen["refusal"])
    assert (effect["verdict"], effect["outputs"][0]["payload"]) == ("ok", b"p")


def test_a_spawn_without_an_id_asks_for_one_and_a_spawn_with_an_id_carries_it(
    fake_core: FakeCore,
) -> None:
    results: list[flezha.SpawnResult] = []

    @flezha.procedure
    def spawner(value: str, ctx: flezha.Ctx[None]) -> None:
        results.append(ctx.spawn_workflow("worker", value).wait())
        results.append(ctx.spawn_workflow("worker", value, id="w1").wait())

    answered(work_activation(91, "p", factory="solo"), solo_deps(spawner, fake_core))
    assert results == [
        flezha.SpawnResult("worker-k7m2qzxw4pd3fn2a", "spawned"),
        flezha.SpawnResult("w1", "spawned"),
    ]
    assert [
        (s["token"], s["factory_id"], s["id"], s["payload"], s["if_running"], s["replay"])
        for s in fake_core.spawns
    ] == [
        (91, "worker", "", codec.encode("p"), "", False),
        (91, "worker", "w1", codec.encode("p"), "", False),
    ]


@pytest.mark.parametrize(
    ("reason", "kind"),
    [
        ("already_running", flezha.SpawnAlreadyRunningError),
        ("factory_unknown", flezha.SpawnFactoryUnknownError),
        ("refused", flezha.SpawnRefusedError),
        ("timeout", flezha.SpawnTimeoutError),
        ("unavailable", flezha.SpawnUnavailableError),
    ],
)
def test_each_failure_a_read_meets_is_its_own_spawn_error(
    fake_core: FakeCore, reason: str, kind: type[flezha.SpawnError]
) -> None:
    fake_core.spawn_answer = {"error": reason, "message": f"the spawn failed: {reason}"}
    raised: list[flezha.SpawnError] = []

    @flezha.procedure
    def spawner(value: str, ctx: flezha.Ctx[None]) -> None:
        handle = ctx.spawn_workflow("worker", value, id="w1", if_running="error")
        try:
            handle.wait()
        except flezha.SpawnError as error:
            raised.append(error)

    (effect,) = answered(work_activation(92, "p", factory="solo"), solo_deps(spawner, fake_core))[
        "effects"
    ]
    (error,) = raised
    assert type(error) is kind
    assert isinstance(error, flezha.FlezhaError)
    assert (error.reason, error.id, str(error)) == (reason, "w1", f"the spawn failed: {reason}")
    assert fake_core.spawns[0]["if_running"] == "error"
    assert effect["verdict"] == "ok"


def test_if_running_without_an_id_is_refused_at_the_call_and_sends_nothing(
    fake_core: FakeCore,
) -> None:
    @flezha.procedure
    def spawner(value: str, ctx: flezha.Ctx[None]) -> None:
        ctx.spawn_workflow("worker", value, if_running="error")

    (effect,) = answered(work_activation(93, "p", factory="solo"), solo_deps(spawner, fake_core))[
        "effects"
    ]
    assert effect["message"].startswith("ValueError: ")
    assert "if_running" in effect["message"]
    assert "id=" in effect["message"]
    assert fake_core.spawns == []


def test_a_replayed_spawn_hands_the_replay_to_the_core_and_answers_the_id_it_was_given(
    fake_core: FakeCore,
) -> None:
    """The core decides a replay sends nothing (asserted in its own suite); what
    the SDK owns is handing it the flag and reading the answer."""
    results: list[flezha.SpawnResult | None] = []

    @flezha.procedure
    def spawner(value: str, ctx: flezha.Ctx[None]) -> None:
        results.append(ctx.spawn_workflow("worker", value).try_result())
        results.append(ctx.spawn_workflow("worker", value, id="w1").try_result())

    activation = work_activation(94, "p", factory="solo")
    activation["items"][0]["replay"] = True
    answered(activation, solo_deps(spawner, fake_core))
    assert results == [
        flezha.SpawnResult("", "skipped_on_replay"),
        flezha.SpawnResult("w1", "skipped_on_replay"),
    ]
    assert [spawn["replay"] for spawn in fake_core.spawns] == [True, True]


def test_an_async_handler_awaits_a_spawn_while_its_loop_keeps_serving(fake_core: FakeCore) -> None:
    fake_core.spawn_answer_after_s = 0.3
    results: list[flezha.SpawnResult] = []

    @flezha.procedure
    async def spawner(value: str, ctx: flezha.Ctx[None]) -> None:
        results.append(await ctx.spawn_workflow("worker", value, id="w1"))

    deps = solo_deps(spawner, fake_core)

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
