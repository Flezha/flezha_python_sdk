"""``call(...)``: the link the builder records, the Response channel it declares
and decodes, the requests an emit makes, and the reference call rows."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable
from typing import Any

import pytest

import app
import contract
import flezha
from conftest import CHECK_CORE, FakeCore, answered, declare, deps_for, echo, wire_work
from flezha import graph
from flezha.codec import ChannelCodec
from flezha.errors import FlezhaError


def _b64(value: bytes | str) -> str:
    return base64.b64encode(value.encode() if isinstance(value, str) else value).decode()


def _caller(codec: ChannelCodec | None) -> flezha.Relay:
    """A caller whose ``ask`` channel calls with ``codec``, the outcome fed back
    into the tasklet that made the call."""

    @flezha.relay(f"caller_{codec}")
    def caller(c: flezha.Channels) -> None:
        (
            c.input("in", codec="text")
            .into(echo)
            .named("ask")
            .filter(lambda line: not line.startswith("drop"))
            .flat_map(lambda line: [line, f"{line}!"])
            .key_by(lambda line: line.split(":", 1)[0])
            .call(factory="agent", deadline_ms=60_000, create=False, channel="turn", codec=codec)
            .map(lambda response: f"answered {response.request}={response.value}@{response.target}")
            .into(echo)
        )

    return caller


def _response_decoder(codec: ChannelCodec | None) -> Callable[[Any], Any]:
    """The decoder of the caller's Response channel, ``ask.response``."""
    decode = _caller(codec).spec.channels[2].decode
    assert decode is not None
    return decode


def test_call_closes_its_chain_with_the_call_link_and_declares_the_response_channel() -> None:
    caller = _caller("text")
    declared = graph.to_host_class_decl(caller.spec)["channels"]
    assert declared[1]["chains"][-1] == [
        {"link": "transform"},
        {"link": "transform"},
        {"link": "key"},
        {
            "link": "call",
            "factory": "agent",
            "deadline_ms": 60_000,
            "create": False,
            "channel": "turn",
            "response": "ask.response",
        },
    ]
    derived = declare(caller)["channels"]
    assert [(c["name"], c["kind"], [e["tasklet"] for e in c["consumers"]]) for c in derived] == [
        ("in", "entry", [0]),
        ("ask", "plugin", []),
        ("ask.response", "entry", [0]),
    ]


def test_a_call_to_a_named_instance_records_its_target_and_create_true() -> None:
    @flezha.relay("named_call")
    def named_call(c: flezha.Channels) -> None:
        c.input("in").into(echo).call("pricing", deadline_ms=500).map(lambda r: r.request).into(
            echo
        )

    assert graph.to_host_class_decl(named_call.spec)["channels"][1]["chains"] == [
        [
            {
                "link": "call",
                "target": "pricing",
                "deadline_ms": 500,
                "create": True,
                "response": "echo.response",
            }
        ]
    ]


def test_a_call_on_an_entry_channel_is_refused_as_a_sink_on_one_is() -> None:
    @flezha.relay("entry_call")
    def entry_call(c: flezha.Channels) -> None:
        c.input("in").call("pricing", deadline_ms=500)

    with pytest.raises(FlezhaError) as refused:
        _ = entry_call.spec
    assert refused.value.code == "topology.sink_on_entry"


def test_every_envelope_case_of_the_shared_fixture_decodes_to_its_expected_fields() -> None:
    decode = _response_decoder("bytes")
    cases = json.loads(contract.member("test/fixtures/call_response_envelope.json").read_text())
    assert cases["cases"]
    for case in cases["cases"]:
        response = decode(case["payload"].encode())
        got: dict[str, Any] = {
            "target": response.target,
            "created": response.created,
            "request": _b64(response.request),
            "ok": response.ok,
        }
        if response.ok:
            got["value"] = _b64(response.value)
        else:
            assert isinstance(response.error, FlezhaError), case["name"]
            got.update(
                error_code=response.error.code,
                error_class=response.error.class_,
                error_message=response.error.message,
            )
        assert got == case["expect"], case["name"]


def test_the_call_codec_reads_a_text_request_and_a_json_answer() -> None:
    def envelope(request: str, value: str) -> bytes:
        return json.dumps(
            {
                "call": "c",
                "target": "anna-42",
                "created": True,
                "request": _b64(request),
                "value": _b64(value),
            }
        ).encode()

    text = _response_decoder("text")(envelope("anna-42:hello", "hi"))
    assert (text.request, text.value) == ("anna-42:hello", "hi")
    as_json = _response_decoder(None)(envelope('"anna-42:hello"', '{"tokens":812}'))
    assert (as_json.request, as_json.value) == ("anna-42:hello", {"tokens": 812})


def test_each_request_the_call_chain_makes_leaves_as_a_plugin_output_keyed_by_the_chain(
    fake_core: FakeCore,
) -> None:
    caller = _caller("text")
    deps = deps_for(caller, fake_core)

    def outputs(channel: int, payload: bytes) -> list[tuple[Any, ...]]:
        (effect,) = answered(wire_work(1, caller.factory_id, (channel, payload)), deps)["effects"]
        return [
            (o["target"], o["plugin_channel"], o["key"], o["payload"]) for o in effect["outputs"]
        ]

    assert outputs(0, b"s1:hi") == [
        ("plugin", "ask", "s1", b"s1:hi"),
        ("plugin", "ask", "s1", b"s1:hi!"),
    ]
    assert outputs(0, b"drop:x") == []
    envelope = {
        "call": "c",
        "target": "s1",
        "created": True,
        "request": _b64("s1:hi"),
        "value": _b64("ok"),
    }
    answered_line = outputs(2, json.dumps(envelope).encode())[0]
    assert answered_line[2:] == ("answered s1", b"answered s1:hi=ok@s1")


def test_the_reference_call_rows_refuse_or_derive_as_the_fixture_states() -> None:
    conformance = app.module("conformance")
    rows = [row for row in conformance.rows() if row["topology"].startswith("call_")]
    assert len(rows) == 7
    table = contract.member("test/fixtures/reference_topologies.txt").read_text().splitlines()
    for row in rows:
        verdict = conformance.judge(row, CHECK_CORE)
        if row["refusal"] != "ok":
            assert row["refusal"] in verdict["names"], row["topology"]
            continue
        lines = [
            " | ".join(
                str(column)
                for column in (
                    row["topology"],
                    c["index"],
                    c["name"],
                    c["kind"],
                    "-" if c["producer"] is None else c["producer"],
                    ",".join(map(str, c["consumers"])) or "-",
                    c["sink"] or "-",
                )
            )
            for c in verdict["derived"]
        ]
        golden = [line.strip() for line in table if line.strip().startswith(f"{row['topology']} |")]
        assert [" | ".join(p.strip() for p in line.split("|")) for line in golden] == lines


def test_the_relay_call_fixtures_the_manifest_lists_pass_the_core() -> None:
    fixtures = app.module("fixtures_call").ALL
    assert [op.factory_id for op in fixtures] == app.declared_fixtures()["relay_call"]
    for relay in fixtures:
        declare(relay)
