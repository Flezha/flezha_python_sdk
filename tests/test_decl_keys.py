"""Every ``HostClassDecl`` key the core learned, spelled by the Python builder.

The fixture at ``test/fixtures/host_class_decl_keys.txt`` is the table of
record; this is the sixth reader of it. A key the core learns that this builder
cannot spell is a red build here, and a key this builder invents that the core
never learned already fails at the core's ``deny_unknown_fields``.
"""

from __future__ import annotations

from typing import Any

import pytest

import app
import contract
import flezha
from conftest import codes, declare, echo
from flezha import graph
from flezha.builder import Link

_FIXTURE = contract.member("test/fixtures/host_class_decl_keys.txt")


def _expected() -> dict[str, set[str]]:
    sections: dict[str, set[str]] = {}
    for line in _FIXTURE.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        section, key = line.split(".", 1)
        sections.setdefault(section, set()).add(key)
    return sections


@flezha.state
class Everything:
    """A state class with a hand-written codec, so the walk has something."""

    def __init__(self) -> None:
        self.value = 0

    def to_dict(self) -> dict[str, int]:
        return {"value": self.value}

    @classmethod
    def from_dict(cls, body: dict[str, int]) -> Everything:
        made = cls()
        made.value = body["value"]
        return made


class Keyed:
    """A dynamic, event-time-clocked, compensating, migrating tasklet.

    Every optional declaration at once, so the emitted keys are the full set
    rather than the subset a simpler fixture would produce.
    """

    def run(self, value: dict[str, int], ctx: flezha.Ctx[Everything, None]) -> None:
        ctx.change({"by": 1})

    def init_state(self, key: str | None) -> Everything:
        return Everything()

    def apply_change(self, state: Everything, change: dict[str, int]) -> None:
        state.value += change["by"]

    def event_time(self, value: dict[str, int]) -> int:
        return value["at"]

    def compensate(self, outputs: list[object], ctx: flezha.Ctx[Everything, None]) -> None:
        return None

    def migrate_state(self, from_version: int, body: bytes) -> Everything:
        return Everything()

    def migrate_change(self, from_version: int, body: bytes) -> dict[str, int]:
        return {"by": 0}


keyed = flezha.tasklet(
    Keyed,
    name="keyed",
    idle_ttl_seconds=60,
    max_cells=1024,
    mailbox_capacity=2048,
    schema_version=3,
    min_readable_schema_version=1,
)


class Statics:
    def run(self, value: dict[str, int], ctx: flezha.Ctx[None, Any]) -> None:
        return None

    def cells_for(self, value: dict[str, int]) -> list[str]:
        return ["a"]


statics = flezha.tasklet(Statics, cell_keys=["a", "b"], name="statics")


@flezha.operator(
    "every_key",
    description="every flow key",
    run={
        "flow": {
            "exactly": {
                "effectively_once": {
                    "ack": {
                        "checkpoint": {"every": 10, "outputs": "at_checkpoint", "max_delay_ms": 500}
                    },
                    "correction_ms": 60_000,
                    "beyond_window": "drop",
                }
            }
        }
    },
    durable={"local": {"sleep_after": 2_000}},
    max_duration_ms=600_000,
)
def every_key(c: flezha.Channels) -> None:
    c.compact_after_bytes(1 << 20)
    c.input("in").key_by(lambda value: str(value["k"])).into(keyed).retry_max(5).retry_backoff_ms(
        100
    ).keyed().sink()


@flezha.operator("static_key")
def static_key(c: flezha.Channels) -> None:
    c.input("in").into(statics).sink()


@flezha.operator(
    "every_steps_key",
    run={"steps": {"each": "chained", "on_failure": "keep_progress"}},
    durable="remote",
    max_duration_ms=600_000,
)
def every_steps_key(c: flezha.Channels) -> None:
    c.input("in").into(echo).sink()


@flezha.operator("every_embedded_key", run={"steps": {"each": "per_input"}})
def every_embedded_key(c: flezha.Channels) -> None:
    c.input("in").into(echo)


@flezha.operator("every_embedding_key")
def every_embedding_key(c: flezha.Channels) -> None:
    c.input("in").into(every_embedded_key, input="in").sink()


def _emitted() -> dict[str, set[str]]:
    """Every key the Python builder can produce, over the fixtures above."""
    emitted: dict[str, set[str]] = {
        "class": set(),
        "channel": set(),
        "tasklet": set(),
    }
    declarations = [
        graph.to_host_class_decl(operator.spec)
        for operator in (every_key, static_key, every_steps_key, every_embedding_key)
    ]
    declarations.append(graph.to_host_class_decl(every_embedded_key.spec, embedded=True))
    for decl in declarations:
        emitted["class"] |= set(decl)
        for channel in decl.get("channels", []):
            emitted["channel"] |= set(channel)
        for tasklet in decl.get("tasklets", []):
            emitted["tasklet"] |= set(tasklet)
    return emitted


def test_the_builder_spells_every_key_the_core_learned() -> None:
    expected = _expected()
    emitted = _emitted()
    for section, keys in expected.items():
        missing = keys - emitted[section]
        assert not missing, (
            f"the Python builder cannot spell {sorted(f'{section}.{key}' for key in missing)}; "
            f"teach it, or the core has a key no Python declaration can carry"
        )


def test_the_builder_invents_no_key_the_core_never_learned() -> None:
    expected = _expected()
    emitted = _emitted()
    for section, keys in emitted.items():
        extra = keys - expected[section]
        assert not extra, (
            f"the Python builder emits {sorted(f'{section}.{key}' for key in extra)}, which the "
            f"fixture does not list; the core refuses unknown fields, so this would fail at "
            f"declare time"
        )


def test_the_fixture_names_the_python_builder_as_a_reader() -> None:
    """A detector nobody knows about is a detector that lapses."""
    text = _FIXTURE.read_text()
    assert "Python" in text or "python" in text


@pytest.mark.parametrize(
    "operator,factory",
    [
        (every_key, "every_key"),
        (static_key, "static_key"),
        (every_steps_key, "every_steps_key"),
    ],
)
def test_each_declaration_carries_the_python_language_byte(
    operator: flezha.Operator, factory: str
) -> None:
    decl = graph.to_host_class_decl(operator.spec)
    assert decl["factory_id"] == factory
    assert decl["sdk_lang"] == 7
    assert decl["change_format_id"] == 5


def test_a_merge_serialises_each_source_as_a_chain_the_merge_link_closes() -> None:
    """The mapping is a `transform` link and the key a `key` link, so the one
    link a merge adds is the terminator the core reads the group off."""
    merged = app.module("fixtures").merged_input_operator
    chains = [channel["chains"] for channel in graph.to_host_class_decl(merged.spec)["channels"]]
    source = [{"link": "transform"}, {"link": "key"}, {"link": "merge", "tasklet": 0, "group": 0}]
    assert chains[0] == chains[1] == [source]


def test_flat_map_records_the_link_map_records() -> None:
    def record(transform: str) -> list[list[Link]]:
        c = flezha.Channels("fan")
        edge = c.input("in")
        getattr(edge, transform)(lambda line: line.split(",")).into(echo)
        return c.channels[0].chains

    assert (
        record("flat_map")
        == record("map")
        == [[{"link": "transform"}, {"link": "into", "tasklet": 0}]]
    )


# --- the cross-language topology golden -------------------------------------
#
# ``test/fixtures/reference_topologies.txt`` is the Rust reference's channel
# topology for two fixtures, transcribed by hand. Four SDKs implement one
# design with no compiler between them, so the file is what keeps them from
# drifting: this builder declares both shapes in Python's spelling and the
# lines must be the golden's, in order.

_TOPOLOGIES = _FIXTURE.parent / "reference_topologies.txt"


def _golden(topology: str) -> list[str]:
    return [
        line
        for line in (raw.strip() for raw in _TOPOLOGIES.read_text().splitlines())
        if line.startswith(f"{topology} |")
    ]


def _lines(topology: str, operator: flezha.Operator) -> list[str]:
    """The DERIVED declaration in the golden's columns, one line per channel.

    The consumers, the kind after the wiring rule, the ``named`` override and the
    sink are all the core's answer to the record this builder submitted.
    """
    rendered = []
    for channel in declare(operator)["channels"]:
        consumers = channel["consumers"]
        rendered.append(
            " | ".join(
                str(field)
                for field in (
                    topology,
                    channel["index"],
                    channel["name"],
                    channel["kind"],
                    "-" if channel["producer"] is None else channel["producer"],
                    ",".join(str(c["tasklet"]) for c in consumers) if consumers else "-",
                    channel["sink"] or "-",
                )
            )
        )
    return rendered


def _bytes(value: object) -> bytes:
    return str(value).encode()


class HybridProducer:
    def run(self, value: str, ctx: flezha.Ctx[None, Any]) -> None:
        ctx.emit(value)


class HybridConsumer:
    def run(self, value: str, ctx: flezha.Ctx[None, Any]) -> None:
        return None


hybrid_producer = flezha.tasklet(HybridProducer, name="hybrid_producer_tasklet")
hybrid_consumer = flezha.tasklet(HybridConsumer, name="hybrid_consumer_tasklet")


@flezha.operator("hybrid_channel")
def hybrid_channel(c: flezha.Channels) -> None:
    c.input("input").into(hybrid_producer).sink("hybrid_indicator", encode=_bytes).into(
        hybrid_consumer
    )


class TierCommitA:
    def run(self, value: str, ctx: flezha.Ctx[None, Any]) -> None:
        ctx.emit(value, key="job")


class BlockB:
    def run(self, value: str, ctx: flezha.Ctx[None, Any]) -> None:
        return None


tier_commit_a = flezha.tasklet(TierCommitA, name="tier_commit_a")
block_b = flezha.tasklet(BlockB, name="block_b")


@flezha.operator("commit_block_chain", run="steps")
def commit_block_chain(c: flezha.Channels) -> None:
    c.input("in").into(tier_commit_a).keyed().sink("mid", encode=_bytes).into(block_b)


def test_the_hybrid_stream_topology_is_the_reference() -> None:
    assert _lines("hybrid_channel", hybrid_channel) == _golden("hybrid_channel")


def test_the_structured_steps_topology_is_the_reference() -> None:
    assert _lines("commit_block_chain", commit_block_chain) == _golden("commit_block_chain")


# --- the cell mode is the wiring's -------------------------------------------


class Plain:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)


plain = flezha.tasklet(Plain, name="plain")


def _modes(operator: flezha.Operator) -> dict[str, str]:
    return {
        tasklet["name"]: tasklet["cell_mode"]
        for tasklet in graph.to_host_class_decl(operator.spec)["tasklets"]
    }


def test_key_by_alone_makes_the_consumer_dynamic_and_one_tasklet_can_be_both() -> None:
    @flezha.operator("keyed_plain")
    def keyed_plain(c: flezha.Channels) -> None:
        c.input("in", codec="text").key_by(lambda value: value[:1]).into(plain).sink()

    @flezha.operator("single_plain")
    def single_plain(c: flezha.Channels) -> None:
        c.input("in", codec="text").into(plain).sink()

    assert _modes(keyed_plain) == {"plain": "dynamic"}
    assert _modes(single_plain) == {"plain": "single"}
    declare(keyed_plain)


def test_a_keyed_merge_makes_the_merged_tasklet_dynamic() -> None:
    @flezha.operator("keyed_merge")
    def keyed_merge(c: flezha.Channels) -> None:
        c.merge().from_(c.input("a"), str).from_(c.input("b"), str).key_by(str).into(plain)

    assert _modes(keyed_merge) == {"plain": "dynamic"}
    declare(keyed_merge)


def test_a_static_key_set_fed_by_key_by_is_refused_by_the_core() -> None:
    @flezha.operator("static_keyed")
    def static_keyed(c: flezha.Channels) -> None:
        c.input("in").key_by(str).into(statics).sink()

    assert _modes(static_keyed) == {"statics": "static"}
    with pytest.raises(flezha.FlezhaError) as caught:
        declare(static_keyed)
    assert codes(caught.value) == ["topology.key_by_on_unkeyable_target"]
