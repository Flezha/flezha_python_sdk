"""The authoring surface, and what the builder still refuses itself.

Every topology rule now lives in ``flz_core::host::register::check``, and
``test/fixtures/reference_topologies.txt`` is where each one's case lives — one
row per refusal, read by every SDK's conformance run. What is left here is the
authoring surface: the shape a declaration builds, the names it derives, which
closure reaches which edge, and the defects a tasklet's own declaration is
refused for when the decorator builds it. The ones a type checker can see are
proved by ``test_typing.py``.
"""

from __future__ import annotations

import socket
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pytest

import app
import contract
import flezha
from conftest import Bumped, Counter, codes, count_up, declare, demo, echo
from flezha import codec, graph
from flezha.builder import apply_transforms
from flezha.errors import FlezhaError
from flezha.testing import TaskletTestHarness

if TYPE_CHECKING:
    from decimal import Decimal

fixtures = app.module("fixtures")


def _derived(operator: flezha.Operator) -> list[tuple[object, ...]]:
    """The channels the core derived, as ``(name, kind, producer, consumers)``."""
    return [
        (
            channel["name"],
            channel["kind"],
            channel["producer"],
            [edge["tasklet"] for edge in channel["consumers"]],
        )
        for channel in declare(operator)["channels"]
    ]


# ---------------------------------------------------------------------------
# The surface itself
# ---------------------------------------------------------------------------


def test_a_class_tasklet_carries_its_hooks() -> None:
    assert count_up.init_state is not None
    assert count_up.apply_change is not None
    assert count_up.on_timer is None
    assert count_up.state_type is Counter
    assert count_up.change_type is Bumped


def test_declaring_performs_no_io(monkeypatch: pytest.MonkeyPatch) -> None:
    """Importing a declaring module opens no socket and starts no thread."""
    opened: list[object] = []
    real_socket = socket.socket

    def watched(*args: object, **kwargs: object) -> object:
        opened.append(args)
        return real_socket(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(socket, "socket", watched)

    @flezha.tasklet(name="later")
    class Later:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

    @flezha.unbounded_operator("late")
    def late(c: flezha.Channels) -> None:
        c.input("in").into(Later).sink()

    declare(late)
    assert opened == []


def test_the_decl_carries_the_python_language_and_codec_bytes() -> None:
    decl = graph.to_host_class_decl(demo.spec)
    assert decl["sdk_lang"] == graph.SDK_LANG_PYTHON == 7
    assert decl["change_format_id"] == graph.FORMAT_ID_MSGPACK == 5


def test_two_unnamed_mentions_of_one_tasklet_are_one_node() -> None:
    @flezha.tasklet(name="split")
    class Split:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

    @flezha.tasklet(name="join")
    class Join:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

    @flezha.unbounded_operator("diamond")
    def diamond(c: flezha.Channels) -> None:
        entry = c.input("in")
        entry.into(Split).named("a").into(Join, "join_a").sink()
        entry.into(Split).named("b").into(Join, "join_b").sink()

    declare(diamond)
    assert [node.name for node in diamond.spec.tasklets] == ["split", "join_a", "join_b"]
    # And one edge: the second bare mention re-declares the first rather than
    # wiring `split` a second time off the same channel.
    assert [e.tasklet for e in diamond.spec.channels[0].consumers] == [0]


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Refusals
# ---------------------------------------------------------------------------


def test_an_unannotated_parameter_is_refused_naming_it() -> None:
    with pytest.raises(FlezhaError) as caught:

        @flezha.tasklet(name="handle")
        class Handle:
            def run(self, order, ctx: flezha.Ctx[None, Any]) -> None:
                ctx.emit(order)

    assert caught.value.code == "topology.invalid_tasklet"
    assert "order" in str(caught.value)
    assert "annotation" in str(caught.value)


def test_a_node_identity_collision_is_refused() -> None:
    @flezha.tasklet(name="shared")
    class First:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

    @flezha.tasklet(name="shared")
    class Second:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

    @flezha.unbounded_operator("collision")
    def collision(c: flezha.Channels) -> None:
        entry = c.input("in")
        entry.into(First).sink()
        entry.into(Second).sink()

    with pytest.raises(FlezhaError) as caught:
        declare(collision)
    # The one refusal this builder raises under a core code.
    assert (caught.value.code, caught.value.details) == (
        "topology.node_identity_collision",
        {"name": "shared"},
    )


# KI-122: three options, one implication. Each writes its own raw flag and the
# core derives the acknowledgement granularity from any of them, so no order of
# calls can clear another's.
def test_the_checkpoint_options_compose_in_any_order_and_none_clears_another() -> None:
    @flezha.tasklet(name="one")
    class One:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

    @flezha.unbounded_operator("compose")
    def compose(c: flezha.Channels) -> None:
        c.effectively_once()
        c.outputs_at_checkpoint(250)
        c.checkpoint_ack()
        c.checkpoint_every(4)
        c.input("in").into(One).keyed().sink()

    assert declare(compose)["ack_granularity"] == "checkpoint"
    decl = graph.to_host_class_decl(compose.spec)

    assert decl["checkpoint_ack"] is True
    assert decl["checkpoint_every"] == 4
    assert decl["checkpoint_outputs"] == "at_checkpoint"
    assert decl["checkpoint_max_delay_ms"] == 250


# The verdict itself: one exception carrying every name the core answered with.
def test_a_refusal_reaches_the_author_under_the_cores_own_names() -> None:
    @flezha.unbounded_operator("edge_already_wired")
    def already_wired(c: flezha.Channels) -> None:
        edge = c.input("in")
        edge.into(echo)
        edge.map(lambda value: f"{value}!").into(echo)

    with pytest.raises(FlezhaError) as caught:
        declare(already_wired)
    assert codes(caught.value) == ["topology.edge_already_wired"]
    assert caught.value.details["tasklet"] == "echo"


# The record carries ``change_is_nochange`` per tasklet: a handler that folds
# no change has the empty Change type, and a logical clock over it never
# advances. The refusal is the core's ``UnservableClock``; what is asserted here
# is that this builder writes the fact it reads.
def test_a_tasklet_that_folds_no_change_declares_it() -> None:
    @flezha.unbounded_operator("nochange")
    def nochange(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink()

    declare(nochange)
    folds_nothing = graph.to_host_class_decl(nochange.spec)["tasklets"][0]
    assert folds_nothing["change_is_nochange"] is True
    folds = graph.to_host_class_decl(demo.spec)["tasklets"][1]
    assert folds["name"] == "count_up"
    assert folds["change_is_nochange"] is False


def test_apply_change_without_state_is_refused_naming_where_state_is_declared() -> None:
    """A tasklet-local defect is refused when the decorator builds it.

    Earlier than the `register_factory` call site and with the traceback on the
    handler — which is why every tasklet-local refusal lives on `Tasklet`
    and `graph.validate` holds only the defects that need the graph.
    """

    class Folding:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

        def apply_change(self, state: dict[str, int], change: dict[str, int]) -> None:
            return None

    with pytest.raises(FlezhaError) as caught:
        flezha.tasklet(Folding, name="folding")
    assert (caught.value.code, caught.value.details) == (
        "topology.change_without_state",
        {"tasklet": "folding"},
    )


def test_an_unrepresentable_state_field_is_refused_naming_it() -> None:
    @flezha.state
    @dataclass
    class WithSocket:
        conn: object = field(default_factory=lambda: socket.socket())

    class Holder:
        def run(self, value: str, ctx: flezha.Ctx[WithSocket, str]) -> None:
            ctx.emit(value)

        def init_state(self, key: str | None) -> WithSocket:
            return WithSocket()

    with pytest.raises(FlezhaError) as caught:
        flezha.tasklet(Holder, name="holder")
    assert caught.value.code == "topology.state_unrepresentable"
    assert caught.value.details["path"] == "holder.state.conn"


def test_a_bounded_that_declares_no_tasklet_is_refused() -> None:
    @flezha.bounded_operator("empty")
    def empty(c: flezha.Channels) -> None:
        return None

    with pytest.raises(FlezhaError) as caught:
        declare(empty)
    assert "topology.no_tasklets" in codes(caught.value)


def test_a_valid_graph_passes() -> None:
    declare(demo)
    assert [node.name for node in demo.spec.tasklets] == ["echo", "count_up"]


# -- one output channel per producing tasklet -------------------------------
#
# The model Rust and Java carry: a tasklet node owns ONE output channel,
# created on first wiring and reused after. A held edge wired twice appends a
# second consumer to it — it does not declare a second channel claiming the same
# producer, which the core resolves by first match (`host/register.rs`
# `tasklet_wiring`) and so silently truncates.


# -- one output channel per producing tasklet -------------------------------
#
# The model Rust and Java carry: a tasklet node owns ONE output channel,
# created on first wiring and reused after. A held edge wired twice appends a
# second consumer to it — it does not declare a second channel claiming the same
# producer, which the core resolves by first match (`host/register.rs`
# `tasklet_wiring`) and so silently truncates.


def test_a_held_edge_wired_twice_is_one_channel_with_two_consumers() -> None:
    @flezha.unbounded_operator("fan_out")
    def fan_out(c: flezha.Channels) -> None:
        edge = c.input("in").into(echo)
        edge.into(count_up, "b")
        edge.into(count_up, "c")

    assert _derived(fan_out) == [
        ("in", "entry", None, [0]),
        ("echo", "inter_task", 0, [1, 2]),
    ]


def test_branch_and_two_into_statements_derive_the_identical_topology() -> None:
    # The record differs by one link — the sugar's declared arity — and the
    # topology the core derives from the two does not.
    @flezha.unbounded_operator("separate")
    def separate(c: flezha.Channels) -> None:
        edge = c.input("in").into(echo)
        edge.into(count_up, "b")
        edge.into(count_up, "c")

    @flezha.unbounded_operator("branched")
    def branched(c: flezha.Channels) -> None:
        c.input("in").into(echo).branch(
            lambda arm: arm.into(count_up, "b"), lambda arm: arm.into(count_up, "c")
        )

    assert _derived(separate) == _derived(branched)


def test_sink_returns_the_edge_so_a_consumer_wires_behind_it() -> None:
    @flezha.unbounded_operator("hybrid")
    def hybrid(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink("hybrid_indicator").into(count_up)

    # A sink alongside a consumer stays inter-tasklet: the value reaches both,
    # and the channel is journalled as the edge it still is.
    assert _derived(hybrid) == [
        ("in", "entry", None, [0]),
        ("hybrid_indicator", "inter_task", 0, [1]),
    ]


def test_the_sink_names_the_channel_and_the_no_argument_form_does_not() -> None:
    @flezha.unbounded_operator("sink_named")
    def named(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink("items_output")

    @flezha.unbounded_operator("sink_derived")
    def derived(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink()

    assert [row[0] for row in _derived(named)] == ["in", "items_output"]
    assert [row[0] for row in _derived(derived)] == ["in", "echo"]


def test_a_channel_named_twice_is_refused_naming_both() -> None:
    @flezha.unbounded_operator("named_twice")
    def named_twice(c: flezha.Channels) -> None:
        c.input("in").into(echo).named("a").sink("b")

    with pytest.raises(FlezhaError) as caught:
        declare(named_twice)
    assert codes(caught.value) == ["topology.channel_named_twice"]
    assert (caught.value.details["named"], caught.value.details["sink"]) == ("a", "b")


def test_a_channel_option_with_nothing_wired_after_it_still_reaches_the_channel() -> None:
    @flezha.unbounded_operator("trailing_named")
    def renamed(c: flezha.Channels) -> None:
        c.input("in").into(echo).named("renamed")

    @flezha.unbounded_operator("trailing_keyed")
    def keyed(c: flezha.Channels) -> None:
        c.input("in").into(echo).keyed()

    assert [row[0] for row in _derived(renamed)] == ["in", "renamed"]
    with pytest.raises(FlezhaError) as caught:
        declare(keyed)
    assert codes(caught.value) == ["topology.keyed_channel_without_plugin_wire"]


def test_two_channels_naming_one_sink_is_refused() -> None:
    @flezha.unbounded_operator("one_sink_two_channels")
    def one_sink(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink("shared").into(count_up).sink("shared")

    with pytest.raises(FlezhaError) as caught:
        declare(one_sink)
    assert codes(caught.value) == ["topology.duplicate_sink_name"]


def test_response_records_its_link_and_ends_the_channel_as_a_sink_would() -> None:
    @flezha.unbounded_operator("answers")
    def terminal(c: flezha.Channels) -> None:
        c.input("in").into(echo).response()

    @flezha.unbounded_operator("answers_and_feeds")
    def hybrid(c: flezha.Channels) -> None:
        c.input("in").into(echo).response().into(count_up)

    assert graph.to_host_class_decl(terminal.spec)["channels"][1]["chains"] == [
        [{"link": "response"}]
    ]
    # A response takes no name, and is a plugin end but not a sink.
    assert _derived(terminal) == [("in", "entry", None, [0]), ("echo", "plugin", 0, [])]
    assert [channel["sink"] for channel in declare(terminal)["channels"]] == [None, None]
    assert _derived(hybrid) == [("in", "entry", None, [0]), ("echo", "inter_task", 0, [1])]


def test_a_sink_beside_a_response_on_one_channel_is_refused() -> None:
    @flezha.unbounded_operator("ends_twice")
    def ends_twice(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink("out").response()

    with pytest.raises(FlezhaError) as caught:
        declare(ends_twice)
    assert codes(caught.value) == ["topology.channel_ends_twice"]
    assert caught.value.details["channel"] == "out"


def test_keyed_reaches_the_channel_of_an_edge_that_also_declares_a_sink() -> None:
    @flezha.unbounded_operator("keyed_hybrid")
    def keyed_hybrid(c: flezha.Channels) -> None:
        c.input("in").into(echo).keyed().sink("mid").into(count_up)

    declare(keyed_hybrid)
    assert graph.to_host_class_decl(keyed_hybrid.spec)["channels"][1]["keyed"] is True


def test_a_transform_before_one_into_reaches_that_consumer_only() -> None:
    @flezha.unbounded_operator("per_edge_transform")
    def per_edge_transform(c: flezha.Channels) -> None:
        edge = c.input("in").into(echo)
        edge.map(lambda value: f"{value}!").into(count_up, "b")
        edge.into(count_up, "c")

    declare(per_edge_transform)
    channel = per_edge_transform.spec.channels[1]
    assert [(e.tasklet, len(e.transforms)) for e in channel.consumers] == [(1, 1), (2, 0)]


# The refusal both references carry by name — Rust's
# `TopologyIssue::DanglingTransformChain` and Java's `danglingTransformChain`.
# A transform lives on the consumer edge it terminates in, so one no `into(...)`
# closed has nowhere to run and would be discarded without a word.


def test_a_bare_re_declared_edge_is_a_no_op_not_a_second_edge() -> None:
    @flezha.unbounded_operator("edge_redeclared_bare")
    def bare(c: flezha.Channels) -> None:
        edge = c.input("in")
        edge.into(echo)
        edge.into(echo)

    declare(bare)
    channel = bare.spec.channels[0]
    assert [(e.tasklet, len(e.transforms)) for e in channel.consumers] == [(0, 0)]


# What an author writes reaches the core whatever the kind, and a setting of
# the other kind is refused there by its own name rather than dropped here.


@pytest.mark.parametrize(
    ("write", "refusal"),
    [
        (lambda c: c.effectively_once(), "topology.effectively_once_on_bounded"),
        (lambda c: c.checkpoint_every(500), "topology.checkpoint_on_bounded"),
        (lambda c: c.correction_window(60_000), "topology.correction_window_on_bounded"),
    ],
)
def test_a_stream_setting_on_a_bounded_is_refused_by_name(
    write: Callable[[flezha.Channels], object], refusal: str
) -> None:
    @flezha.bounded_operator("unbounded_setting_bounded")
    def bounded(c: flezha.Channels) -> None:
        write(c)
        c.input("in").into(echo)

    with pytest.raises(FlezhaError) as refused:
        declare(bounded)
    assert codes(refused.value) == [refusal]


def test_sync_durability_on_a_stream_is_refused_by_name() -> None:
    @flezha.unbounded_operator("sync_unbounded", sync_durability=True)
    def stream(c: flezha.Channels) -> None:
        c.input("in").into(echo).sink("out")

    with pytest.raises(FlezhaError) as refused:
        declare(stream)
    assert codes(refused.value) == ["topology.sync_durability_on_unbounded"]


@pytest.mark.parametrize(
    ("decorator", "persistent", "sleeps"),
    [
        (flezha.bounded_operator, True, True),
        (flezha.bounded_operator, False, False),
        (flezha.unbounded_operator, True, False),
    ],
)
def test_sleeping_defaults_to_persistence_on_a_bounded_only(
    decorator: Callable[..., Any], persistent: bool, sleeps: bool
) -> None:
    @decorator("sleep_default", persistent=persistent)
    def operator(c: flezha.Channels) -> None:
        c.input("in").into(echo)

    assert graph.to_host_class_decl(operator.spec)["sleep_when_idle"] is sleeps


@pytest.mark.parametrize("decorator", [flezha.unbounded_operator, flezha.bounded_operator])
def test_sleeping_without_persistence_is_refused_by_name(decorator: Callable[..., Any]) -> None:
    @decorator("sleep_no_persist", persistent=False, sleep_when_idle=True)
    def operator(c: flezha.Channels) -> None:
        c.input("in").into(echo)

    with pytest.raises(FlezhaError) as caught:
        declare(operator)
    assert codes(caught.value) == ["topology.sleep_without_persistence"]


@pytest.mark.parametrize(
    ("keyword", "value"), [("loop_mode", "forever"), ("rollback_mode", "best_effort")]
)
def test_a_mode_outside_its_declared_values_is_refused_at_decoration(
    keyword: str, value: str
) -> None:
    written: dict[str, Any] = {keyword: value}
    with pytest.raises(FlezhaError) as caught:
        flezha.bounded_operator(id="w", **written)
    assert (caught.value.code, caught.value.details) == (
        "topology.invalid_mode",
        {"factory_id": "w", "knob": keyword, "value": value},
    )


# ---------------------------------------------------------------------------
# Merge, the latest-document router and flat_map
# ---------------------------------------------------------------------------


def test_two_sources_merge_into_one_node_each_edge_under_its_own_mapping() -> None:
    merged = fixtures.merged_input_operator
    assert _derived(merged) == [
        ("ticks", "entry", None, [0]),
        ("quotes", "entry", None, [0]),
        ("merged", "plugin", 0, []),
    ]
    ticks, quotes = merged.spec.channels[:2]
    for channel, source in ((ticks, "ticks"), (quotes, "quotes")):
        (edge,) = channel.consumers
        (value,) = apply_transforms(edge.transforms, "AAPL:1")
        assert value == {"source": source, "symbol": "AAPL", "payload": "AAPL:1"}
        # The merge's one key, evaluated on the mapped value.
        assert edge.key_by is not None and edge.key_by(value) == "AAPL"


def _merge_refusal(sources: int) -> FlezhaError:
    """The verdict on a merge naming one entry edge ``sources`` times."""

    @flezha.unbounded_operator("merge_refused")
    def refused(c: flezha.Channels) -> None:
        merge, entry = c.merge(), c.input("in")
        for _ in range(sources):
            merge.from_(entry, fixtures.tagged("in"))
        merge.into(echo)

    with pytest.raises(FlezhaError) as caught:
        declare(refused)
    return caught.value


def test_a_merge_of_one_source_is_refused_by_the_reference_name() -> None:
    assert codes(_merge_refusal(1)) == ["topology.merge_too_few_sources"]


def test_a_merge_naming_one_source_twice_is_refused_naming_the_channel() -> None:
    refusal = _merge_refusal(2)
    (issue,) = [
        i for i in refusal.details["issues"] if i["code"] == "topology.duplicate_merge_source"
    ]
    assert issue["details"]["channel"] == "in"


def test_a_merge_source_already_wired_into_the_tasklet_is_refused() -> None:
    @flezha.unbounded_operator("merge_rewired")
    def rewired(c: flezha.Channels) -> None:
        entry = c.input("in")
        entry.map(str.upper).into(echo)
        c.merge().from_(entry, str.lower).from_(c.input("other"), str.lower).into(echo)

    with pytest.raises(FlezhaError) as caught:
        declare(rewired)
    assert codes(caught.value) == ["topology.edge_already_wired"]


@flezha.tasklet(name="routed_document")
class RoutedDocument:
    def run(self, value: Any, ctx: flezha.Ctx[None, None]) -> None:
        pass


def test_the_router_is_one_route_latest_node_and_a_named_router_is_another() -> None:
    @flezha.unbounded_operator("routers")
    def routers(c: flezha.Channels) -> None:
        trades = c.input("trades")
        c.route_latest(c.input("config"), trades, lambda trade: trade["symbol"]).into(
            RoutedDocument
        )
        c.route_latest_named(
            c.input("feature_flags"), trades, lambda trade: trade["desk"], "flags"
        ).into(RoutedDocument)

    declare(routers)
    assert [node.name for node in routers.spec.tasklets] == [
        "route_latest",
        "routed_document",
        "flags",
    ]
    assert graph.to_host_class_decl(routers.spec)["tasklets"][0]["cell_mode"] == "single"


def test_two_unnamed_routers_are_one_node_so_a_shared_traffic_edge_is_refused() -> None:
    @flezha.unbounded_operator("unnamed_routers")
    def routers(c: flezha.Channels) -> None:
        trades = c.input("trades")
        for documents in ("config", "feature_flags"):
            c.route_latest(c.input(documents), trades, lambda trade: trade["symbol"])

    with pytest.raises(FlezhaError) as caught:
        declare(routers)
    assert codes(caught.value) == ["topology.edge_already_wired"]
    assert [node.name for node in routers.spec.tasklets] == ["route_latest"]


def test_flat_map_fans_a_value_out_and_an_empty_fan_is_a_drop() -> None:
    edge = flezha.Channels("fan").input("in").flat_map(lambda line: line.split(",")).map(str.upper)
    assert apply_transforms(edge._transforms, "a,b,c") == ["A", "B", "C"]
    empty = flezha.Channels("fan").input("in").flat_map(lambda line: [])
    assert apply_transforms(empty._transforms, "a") == []
    # A map answering a list is one value, not a fan.
    assert apply_transforms([lambda line: line.split(",")], "a,b") == [["a", "b"]]


# ---------------------------------------------------------------------------
# Channel codecs
# ---------------------------------------------------------------------------
#
# A codec is resolved when the class is declared, into the two slots the
# dispatcher and the plugin frame already read. What every SDK sends alike is
# ``test/fixtures/channel_codecs.txt``; what is Python's own is asserted here.

_CODEC_CASES = contract.member("test/fixtures/channel_codecs.txt")


def _codec_slots(
    input_options: dict[str, Any] | None = None, sink_options: dict[str, Any] | None = None
) -> tuple[Callable[[Any], Any], Callable[[Any], bytes]]:
    channels = flezha.Channels("codecs")
    channels.input("in", **(input_options or {})).into(echo).sink("out", **(sink_options or {}))
    entry, out = channels.channels
    assert entry.decode is not None and out.sink_encode is not None
    return entry.decode, out.sink_encode


def _case_bytes(field: str) -> bytes:
    if field == "<empty>":
        return b""
    return bytes.fromhex(field.removeprefix("hex:")) if field.startswith("hex:") else field.encode()


def _case_outcome(
    decode: Callable[[Any], Any], encode: Callable[[Any], bytes], payload: bytes
) -> bytes | str:
    # A case records that a step refused, whatever it raised.
    try:
        value = decode(payload)
    except Exception:  # noqa: BLE001
        return "decode_error"
    try:
        return encode(value)
    except Exception:  # noqa: BLE001
        return "encode_error"


def test_every_shared_codec_case_sends_the_bytes_the_fixture_records() -> None:
    rows = [row for row in _CODEC_CASES.read_text().splitlines() if row and not row.startswith("#")]
    assert rows, "the shared codec cases were read"
    for row in rows:
        entry, sink, payload, expected = row.split("|", 3)
        decode, encode = _codec_slots(
            {} if entry == "none" else {"codec": entry}, {} if sink == "none" else {"codec": sink}
        )
        want = expected if expected.endswith("_error") else _case_bytes(expected)
        assert _case_outcome(decode, encode, _case_bytes(payload)) == want, row


def test_a_json_entry_reads_a_dict_and_a_sink_takes_every_bytes_type() -> None:
    decode, encode = _codec_slots()
    assert decode(b'{"a":1}') == {"a": 1}
    assert encode(bytearray(b"\xff")) == b"\xff"
    with pytest.raises(FlezhaError) as caught:
        encode(object())
    assert (caught.value.code, caught.value.details) == (
        "runtime.value_not_json",
        {"owner": "channel 'out'", "type": "object", "reason": caught.value.details["reason"]},
    )


def test_msgpack_is_the_class_codec() -> None:
    decode, encode = _codec_slots({"codec": "msgpack"}, {"codec": "msgpack"})
    assert decode(codec.encode({"a": 1})) == {"a": 1}
    assert encode({"a": 1}) == codec.encode({"a": 1})


def test_a_refusal_names_the_channel_and_a_bytes_sink_refuses_a_string() -> None:
    """A Python ``str`` is not bytes, so a ``bytes`` sink refuses one, where Ruby
    and PHP take a string there."""
    for codec_name, value in (("text", 42), ("bytes", 42), ("bytes", "hello")):
        encode = _codec_slots(sink_options={"codec": codec_name})[1]
        with pytest.raises(FlezhaError) as caught:
            encode(value)
        assert (caught.value.code, caught.value.details) == (
            "runtime.value_wrong_for_codec",
            {"channel": "out", "codec": codec_name, "type": type(value).__name__},
        )


def test_an_unknown_codec_name_is_refused_naming_the_operator_the_channel_and_the_names() -> None:
    for input_options, sink_options, channel in (
        ({"codec": "yaml"}, {}, "in"),
        ({}, {"codec": "yaml"}, "out"),
    ):
        with pytest.raises(FlezhaError) as caught:
            _codec_slots(input_options, sink_options)
        assert (caught.value.code, caught.value.details) == (
            "topology.unknown_codec",
            {"channel": channel, "codec": "yaml", "codecs": ["bytes", "text", "json", "msgpack"]},
        )


def test_a_codec_and_a_callable_together_are_refused() -> None:
    for input_options, sink_options, channel in (
        ({"codec": "text", "decode": bytes}, {}, "in"),
        ({}, {"codec": "text", "encode": bytes}, "out"),
    ):
        with pytest.raises(FlezhaError) as caught:
            _codec_slots(input_options, sink_options)
        assert (caught.value.code, caught.value.details) == (
            "topology.codec_with_callable",
            {"channel": channel},
        )


# ---------------------------------------------------------------------------
# The §5 short form
# ---------------------------------------------------------------------------


def _decodes_text(operator: flezha.Operator) -> bool:
    decode = operator.spec.channels[0].decode
    return decode is not None and decode(b"abc") == "abc"


def test_an_unbounded_short_form_is_its_hand_written_twin() -> None:
    @flezha.unbounded_operator(id="upcase", sink="out", codec="text")
    def upcase(value: str) -> str:
        return value.upper()

    @flezha.tasklet(name="upcase")
    class UpcaseTasklet:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value.upper())

    @flezha.unbounded_operator("upcase")
    def twin(c: flezha.Channels) -> None:
        c.input("input", codec="text").into(UpcaseTasklet).sink("out")

    assert upcase.kind == "unbounded"
    assert graph.to_host_class_decl(upcase.spec) == graph.to_host_class_decl(twin.spec)
    assert _decodes_text(upcase)


@pytest.mark.parametrize(
    ("written", "loop_mode"), [({}, "once"), ({"loop_mode": "per_input"}, "per_input")]
)
def test_a_bounded_short_form_is_its_hand_written_twin(
    written: dict[str, Any], loop_mode: str
) -> None:
    @flezha.bounded_operator(id="place", sink="placed", input="order", codec="text", **written)
    def place(order: str) -> str:
        return order

    @flezha.tasklet(name="place")
    class PlaceTasklet:
        def run(self, order: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(order)

    @flezha.bounded_operator("place", **written)
    def twin(c: flezha.Channels) -> None:
        c.input("order", codec="text").into(PlaceTasklet).sink("placed")

    assert place.kind == "bounded"
    record = graph.to_host_class_decl(place.spec)
    assert record == graph.to_host_class_decl(twin.spec)
    assert record["bounded"]["loop_mode"] == loop_mode
    declare(place)


def test_a_short_form_with_no_sink_declares_its_entry_and_its_tasklet_only() -> None:
    state_only = fixtures.SugarStateOnly
    assert _derived(state_only) == [("ticks", "entry", None, [0])]
    assert [node.name for node in state_only.spec.tasklets] == ["sugar_state_only"]
    assert _decodes_text(state_only)


def test_a_channels_body_annotated_or_not_is_still_the_full_form() -> None:
    def bare(c) -> None:
        c.input("input").into(echo).sink("out")

    @flezha.unbounded_operator(id="static_cell")
    def static_cell(c: flezha.Channels) -> None:
        bare(c)

    for full in (static_cell, flezha.unbounded_operator(id="bare")(bare)):
        assert full.tasklet is None
        assert [node.name for node in full.spec.tasklets] == ["echo"]


def test_a_function_short_form_taking_a_context_is_refused() -> None:
    with pytest.raises(FlezhaError) as caught:

        @flezha.unbounded_operator(id="stamp", sink="out")
        def stamp(value: str, ctx: flezha.Ctx[None, str]) -> str:
            return value

    assert caught.value.code == "topology.invalid_tasklet"
    assert "`ctx`" in str(caught.value)
    assert "class whose `run(self, value, ctx)`" in str(caught.value)


def test_the_generated_tasklet_composes_in_another_topology() -> None:
    @flezha.unbounded_operator(id="upcase", sink="out")
    def upcase(value: str) -> str:
        return value.upper()

    generated = upcase.tasklet
    assert generated is not None
    assert generated("abc") == "ABC"

    @flezha.unbounded_operator("wider")
    def wider(c: flezha.Channels) -> None:
        c.input("in").into(generated).into(echo).sink("out")

    declare(wider)
    assert [node.name for node in wider.spec.tasklets] == ["upcase", "echo"]


def test_a_tasklet_body_beside_a_topology_body_is_refused() -> None:
    class Both:
        def run(self, value: str) -> str:
            return value

        def channels(self, c: flezha.Channels) -> None:
            return None

    with pytest.raises(FlezhaError) as caught:
        flezha.unbounded_operator(id="both", sink="out")(Both)

    assert (caught.value.code, caught.value.details) == (
        "topology.short_form_with_topology",
        {"factory_id": "both"},
    )
    assert "(topology.short_form_with_topology)" in str(caught.value)


def _returns_none(value: str) -> None:
    del value


def _returns_unannotated(value: str):
    return value


# `Decimal` is imported for the type checker only, so the hints cannot resolve
# at run time and the return reaches the check as the raw annotation.
def _returns_none_under_an_unresolvable_hint(value: Decimal) -> None:
    del value


@pytest.mark.parametrize(
    "body", [_returns_none, _returns_unannotated, _returns_none_under_an_unresolvable_hint]
)
def test_a_sink_on_a_body_with_no_output_is_refused(body: Callable[[str], Any]) -> None:
    with pytest.raises(FlezhaError) as caught:
        flezha.unbounded_operator(id="drain", sink="out")(body)

    assert (caught.value.code, caught.value.details) == (
        "topology.sink_without_output",
        {"factory_id": "drain"},
    )


def test_an_absent_id_is_refused_rather_than_derived() -> None:
    with pytest.raises(FlezhaError) as short:

        @flezha.unbounded_operator(sink="out")
        def upcase(value: str) -> str:
            return value

    with pytest.raises(FlezhaError) as full_form:

        @flezha.bounded_operator()
        def full(c: flezha.Channels) -> None:
            c.input("in").into(echo).sink("out")

    for caught, name in ((short, "upcase"), (full_form, "full")):
        assert (caught.value.code, caught.value.details) == (
            "topology.missing_factory_id",
            {"operator": name},
        )


@pytest.mark.parametrize("keyword", ["kind", "sink", "input", "codec"])
def test_an_operator_keyword_on_a_tasklet_is_refused(keyword: str) -> None:
    declarations: dict[str, Any] = {keyword: "text"}
    with pytest.raises(FlezhaError) as caught:

        @flezha.tasklet(name="upcase", **declarations)
        class Upcase:
            def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
                ctx.emit(value)

    assert caught.value.code == "topology.operator_field_on_tasklet"
    assert caught.value.details["fields"] == [f"`{keyword}`"]
    assert "flezha.unbounded_operator" in str(caught.value)


def test_a_short_form_carries_its_tasklets_own_settings() -> None:
    class Keyed:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value)

        def cells_for(self, value: str) -> list[str]:
            return ["a"]

    short = flezha.unbounded_operator(
        id="keyed", sink="out", cell_keys=["a", "b"], mailbox_capacity=8
    )(Keyed)
    keyed = flezha.tasklet(name="keyed", cell_keys=["a", "b"], mailbox_capacity=8)(Keyed)

    @flezha.unbounded_operator("keyed")
    def twin(c: flezha.Channels) -> None:
        c.input("input").into(keyed).sink("out")

    assert short.tasklet is not None
    assert short.tasklet.cell_keys == ["a", "b"]
    assert short.tasklet.mailbox_capacity == 8
    assert graph.to_host_class_decl(short.spec) == graph.to_host_class_decl(twin.spec)
    declare(short)


@pytest.mark.parametrize("keyword", [{"sink": "out"}, {"mailbox_capacity": 8}])
def test_a_short_form_keyword_on_a_channels_body_is_refused(keyword: dict[str, Any]) -> None:
    with pytest.raises(FlezhaError) as caught:

        @flezha.unbounded_operator(id="full", **keyword)
        def full(c: flezha.Channels) -> None:
            c.input("input").into(echo).sink("out")

    assert (caught.value.code, caught.value.details) == (
        "topology.short_form_field_on_full_form",
        {"factory_id": "full", "fields": [f"`{next(iter(keyword))}`"]},
    )


@pytest.mark.parametrize(
    ("declare_operator", "keyword"),
    [
        (flezha.unbounded_operator, "name"),
        (flezha.bounded_operator, "retry_attempts"),
        (flezha.bounded_operator, "retry_backoff_ms"),
        (flezha.bounded_operator, "barrier_retry_attempts"),
        (flezha.bounded_operator, "wake_after_ms"),
    ],
)
def test_a_keyword_neither_form_takes_is_a_type_error(
    declare_operator: Callable[..., Any], keyword: str
) -> None:
    written: dict[str, Any] = {keyword: 5}
    with pytest.raises(TypeError, match=keyword):
        declare_operator(id="upcase", **written)


@pytest.mark.parametrize(
    ("declare_operator", "setting"),
    [
        (flezha.unbounded_operator, {"persistent": False}),
        (flezha.unbounded_operator, {"sync_durability": True}),
        (flezha.bounded_operator, {"rollback_mode": "keep_progress"}),
        (flezha.bounded_operator, {"max_duration_ms": 60_000}),
    ],
)
def test_a_class_setting_on_a_short_form_is_refused(
    declare_operator: Callable[..., Any], setting: dict[str, Any]
) -> None:
    def place(order: str) -> str:
        return order

    with pytest.raises(FlezhaError) as caught:
        declare_operator(id="place", sink="placed", **setting)(place)
    assert (caught.value.code, caught.value.details) == (
        "topology.setting_on_short_form",
        {"factory_id": "place", "fields": [f"`{next(iter(setting))}`"]},
    )


def test_a_class_tasklet_body_is_its_run_method() -> None:
    @flezha.tasklet(name="upcase")
    class Upcase:
        def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
            ctx.emit(value.upper())

    assert TaskletTestHarness(Upcase).feed("abc").outputs == ["ABC"]


def test_a_class_with_a_process_body_and_no_run_is_refused_naming_run() -> None:
    with pytest.raises(FlezhaError) as caught:

        @flezha.tasklet(name="upcase")  # type: ignore[arg-type]
        class Upcase:
            def process(self, value: str) -> str:
                return value.upper()

    assert (caught.value.code, caught.value.details) == (
        "topology.missing_body",
        {"declaration": "tasklet 'Upcase'", "body": "`run`"},
    )
