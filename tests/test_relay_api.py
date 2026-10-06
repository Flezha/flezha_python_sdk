"""A relay's API: the format, schema and description each channel announces."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, NotRequired, TypedDict

import pytest

import app
import flezha
from conftest import codes, declare
from flezha import codec, graph
from flezha.errors import FlezhaError


class Side(enum.Enum):
    BUY = "buy"
    SELL = "sell"


@dataclass
class Leg:
    venue: str
    weight: float


@dataclass
class Order:
    pair: str
    amount: float
    lots: int
    urgent: bool
    side: Side
    tags: list[str]
    legs: dict[str, Leg]
    main: Leg
    memo: str | None = None
    notes: list[str] = field(default_factory=list)


class Quote(TypedDict):
    pair: str
    amount: float
    note: NotRequired[str]


class Modelled:
    """Any type with ``model_json_schema()``, as a pydantic model has."""

    @classmethod
    def model_json_schema(cls) -> dict[str, Any]:
        return {"type": "object", "title": "Modelled"}


@dataclass
class Node:
    name: str
    children: list[Node]


def test_a_dataclass_derives_every_supported_field_type() -> None:
    leg = {
        "type": "object",
        "properties": {"venue": {"type": "string"}, "weight": {"type": "number"}},
        "required": ["venue", "weight"],
    }
    assert codec.json_schema(Order) == {
        "type": "object",
        "properties": {
            "pair": {"type": "string"},
            "amount": {"type": "number"},
            "lots": {"type": "integer"},
            "urgent": {"type": "boolean"},
            "side": {"enum": ["buy", "sell"]},
            "tags": {"type": "array", "items": {"type": "string"}},
            "legs": {"type": "object", "additionalProperties": leg},
            "main": leg,
            "memo": {"type": "string"},
            "notes": {"type": "array", "items": {"type": "string"}},
        },
        "required": ["pair", "amount", "lots", "urgent", "side", "tags", "legs", "main"],
    }


def test_a_typed_dict_honours_its_required_keys() -> None:
    assert codec.json_schema(Quote) == {
        "type": "object",
        "properties": {
            "pair": {"type": "string"},
            "amount": {"type": "number"},
            "note": {"type": "string"},
        },
        "required": ["pair", "amount"],
    }


def test_a_type_with_model_json_schema_gives_its_own() -> None:
    assert codec.json_schema(Modelled) == {"type": "object", "title": "Modelled"}


def test_a_type_that_contains_itself_stops_one_level_down() -> None:
    schema = codec.json_schema(Node)
    assert schema is not None
    assert schema["properties"]["children"] == {"type": "array", "items": {}}


def test_any_other_type_derives_no_schema() -> None:
    assert codec.json_schema(dict[str, int]) is None
    assert codec.json_schema(str) is None
    assert codec.json_schema(None) is None


class Place:
    def run(self, order: Quote, ctx: flezha.Ctx[None, Order]) -> None:
        return None


class Audit:
    def run(self, line: str, ctx: flezha.Ctx[None, None]) -> None:
        return None


place = flezha.tasklet(Place, name="api_place")
audit = flezha.tasklet(Audit, name="api_audit")


@flezha.relay("api_facts")
def api_facts(c: flezha.Channels) -> None:
    c.input("place", description="Places an order").into(place).output("placed")
    c.input("declared", schema={"type": "object", "required": ["pair"]}).into(place)
    c.input("audit", codec="text").into(audit)
    c.input("parsed", decode=bytes).into(audit)
    c.input("mapped").map(lambda value: value).into(place)


def test_each_channel_announces_its_format_schema_and_description() -> None:
    declare(api_facts)
    channels = {c["name"]: c for c in graph.to_host_class_decl(api_facts.spec)["channels"]}

    assert channels["place"]["codec"] == "json"
    assert channels["place"]["description"] == "Places an order"
    assert channels["place"]["schema"] == codec.json_schema(Quote)
    assert channels["declared"]["schema"] == {"type": "object", "required": ["pair"]}
    assert channels["audit"]["codec"] == "text"
    assert "schema" not in channels["audit"]
    assert "description" not in channels["audit"]
    assert channels["parsed"]["codec"] == "custom"
    assert "schema" not in channels["mapped"], "a transform hides the type the wire carries"
    assert channels["placed"]["codec"] == "json"
    assert channels["placed"]["schema"] == codec.json_schema(Order)


def test_a_declared_schema_that_is_not_an_object_with_a_type_is_refused() -> None:
    @flezha.relay("api_broken_schema")
    def broken(c: flezha.Channels) -> None:
        c.input("place", schema={"properties": {}}).into(place)

    with pytest.raises(FlezhaError) as caught:
        declare(broken)
    assert codes(caught.value) == ["topology.schema_invalid"]
    (issue,) = caught.value.details["issues"]
    assert issue["details"]["channel"] == "place"


def test_the_relay_api_fixtures_the_manifest_lists_pass_the_core() -> None:
    fixtures = app.module("fixtures_call").API
    assert [relay.factory_id for relay in fixtures] == app.declared_fixtures()["relay_api"]
    for relay in fixtures:
        declare(relay)
