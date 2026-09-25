"""Every ``HostClassDecl`` key the core learned, spelled by the Python builder.

The fixture at ``test/fixtures/host_class_decl_keys.txt`` is the table of
record; this is the sixth reader of it. A key the core learns that this builder
cannot spell is a red build here, and a key this builder invents that the core
never learned already fails at the core's ``deny_unknown_fields``.
"""

from __future__ import annotations

import dataclasses

import pytest

import app
import contract
import flezha
from conftest import declare, echo
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

    def run(self, value: dict[str, int], ctx: flezha.Ctx[Everything]) -> None:
        ctx.change({"by": 1})

    def initial_state(self, key: str | None) -> Everything:
        return Everything()

    def apply_change(self, state: Everything, change: dict[str, int]) -> None:
        state.value += change["by"]

    def event_time(self, value: dict[str, int]) -> int:
        return value["at"]

    def compensate(self, outputs: list[object], ctx: flezha.Ctx[Everything]) -> None:
        return None

    def migrate_state(self, from_version: int, body: bytes) -> Everything:
        return Everything()

    def migrate_change(self, from_version: int, body: bytes) -> dict[str, int]:
        return {"by": 0}


keyed = flezha.tasklet(
    Keyed,
    clock="event_time",
    dynamic_cells=True,
    idle_ttl_ms=60_000,
    max_cells=1024,
    mailbox_capacity=2048,
    schema_version=3,
    min_readable_schema_version=1,
)


class Statics:
    def run(self, value: dict[str, int], ctx: flezha.Ctx[None]) -> None:
        return None

    def cells_for(self, value: dict[str, int]) -> list[str]:
        return ["a"]


statics = flezha.tasklet(Statics, cell_keys=["a", "b"], name="statics")


@flezha.unbounded_operator("every_key")
def every_key(c: flezha.Channels) -> None:
    c.effectively_once().checkpoint_every(10).outputs_at_checkpoint(500)
    c.correction_window(60_000).beyond_window_policy("drop").compact_after_bytes(1 << 20)
    c.input("in").key_by(lambda value: str(value["k"])).into(keyed).retry_max(5).retry_backoff_ms(
        100
    ).keyed().sink()


@flezha.unbounded_operator("static_key")
def static_key(c: flezha.Channels) -> None:
    c.input("in").into(statics).sink()


@flezha.bounded_operator(
    "every_bounded_key",
    loop_mode="chained",
    rollback_mode="keep_progress",
    max_duration_ms=600_000,
    sync_durability=True,
)
def every_bounded_key(c: flezha.Channels) -> None:
    c.input("in").into(echo).sink()


def _emitted() -> dict[str, set[str]]:
    """Every key the Python builder can produce, over the fixtures above."""
    emitted: dict[str, set[str]] = {
        "class": set(),
        "channel": set(),
        "tasklet": set(),
        "bounded": set(),
    }
    for operator in (every_key, static_key, every_bounded_key):
        decl = graph.to_host_class_decl(operator.spec)
        emitted["class"] |= set(decl)
        for channel in decl.get("channels", []):
            emitted["channel"] |= set(channel)
        for tasklet in decl.get("tasklets", []):
            emitted["tasklet"] |= set(tasklet)
        emitted["bounded"] |= set(decl.get("bounded") or {})
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
        (every_bounded_key, "every_bounded_key"),
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


# --- what the author wrote reaches the core, whatever the kind -------------
#
# ``flz_core::host::register::refuse_bounded_facts`` names each unbounded-only
# key a bounded class may not carry. This builder puts every one on the wire
# for either kind, so the refusal is the core's, by name, and never a drop.

_UNBOUNDED_ONLY = (
    "effectively_once",
    "checkpoint_ack",
    "checkpoint_every",
    "checkpoint_outputs",
    "checkpoint_max_delay_ms",
    "correction_window_ms",
    "beyond_window_policy",
)


def test_every_unbounded_setting_reaches_the_wire_on_a_bounded() -> None:
    """The unbounded fixture's spec re-kinded: every stream-only key populated at once."""
    bounded = dataclasses.replace(
        every_key.spec,
        factory_id="tier_bounded",
        kind="bounded",
        bounded=every_bounded_key.spec.bounded,
    )
    decl = graph.to_host_class_decl(bounded)
    dropped = [key for key in _UNBOUNDED_ONLY if key not in decl]
    assert not dropped, f"keys dropped before the core could judge them: {dropped}"


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
    def run(self, value: str, ctx: flezha.Ctx[None]) -> None:
        ctx.emit(value)


class HybridConsumer:
    def run(self, value: str, ctx: flezha.Ctx[None]) -> None:
        return None


hybrid_producer = flezha.tasklet(HybridProducer, name="hybrid_producer_tasklet")
hybrid_consumer = flezha.tasklet(HybridConsumer, name="hybrid_consumer_tasklet")


@flezha.unbounded_operator("hybrid_channel")
def hybrid_channel(c: flezha.Channels) -> None:
    c.input("input").into(hybrid_producer).sink("hybrid_indicator", encode=_bytes).into(
        hybrid_consumer
    )


class TierCommitA:
    def run(self, value: str, ctx: flezha.Ctx[None]) -> None:
        ctx.emit(value, key="job")


class BlockB:
    def run(self, value: str, ctx: flezha.Ctx[None]) -> None:
        return None


tier_commit_a = flezha.tasklet(TierCommitA, name="tier_commit_a")
block_b = flezha.tasklet(BlockB, name="block_b")


@flezha.bounded_operator("commit_block_chain")
def commit_block_chain(c: flezha.Channels) -> None:
    c.input("in").into(tier_commit_a).keyed().sink("mid", encode=_bytes).into(block_b)


def test_the_hybrid_stream_topology_is_the_reference() -> None:
    assert _lines("hybrid_channel", hybrid_channel) == _golden("hybrid_channel")


def test_the_structured_bounded_topology_is_the_reference() -> None:
    assert _lines("commit_block_chain", commit_block_chain) == _golden("commit_block_chain")
