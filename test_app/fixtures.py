"""Twins of the rust_runner e2e fixtures (``test/apps/rust_runner/src/main.rs``)
under the same factory ids, tasklet names, channel names and output bytes.

Entry payloads and plugin frames are UTF-8 text on the wire, the same as the
reference: every entry names the text codec, and a sink naming no codec sends a
string as its UTF-8 bytes and a bytes value unchanged. An inter-tasklet hop
between two Python tasklets carries MessagePack, which no one else reads.
"""

from __future__ import annotations

import struct
import sys
import time
from collections.abc import Callable
from typing import Any, ClassVar

import flezha
from fixture_support import _as_int, _env_int, passthrough, prefix
from fixtures_bounded import stateless_loop_bounded
from fixtures_state import (
    bench_checkpoint_operator,
    bench_keyed_nopersist_operator,
    bench_keyed_operator,
    checkpoint_batched_operator,
    checkpoint_store_operator,
    coalescing_counter_operator,
    persistence_off_operator,
    resume_probe_operator,
    schema_evolution_v1_operator,
    schema_evolution_v2_operator,
    sleeping_counter_operator,
    stateful_counter_operator,
    store_input_operator,
    tier_store_operator,
)

# -- do_nothing / bench_echo -------------------------------------------------

do_nothing = passthrough("do_nothing_tasklet")


@flezha.unbounded_operator("do_nothing")
def do_nothing_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(do_nothing).sink("echo")


@flezha.unbounded_operator("bench_echo")
def bench_echo_operator(c: flezha.Channels) -> None:
    """Echo with a declared DropNothing on its entry mailbox.

    ``do_nothing`` is the merge-blocking bench factory and must not change; this
    keeps the round-trip bench's back-pressure posture independent of it.
    """
    c.input("input", codec="text").on_full("drop_nothing").into(do_nothing).sink("echo")


# -- compare_filter ----------------------------------------------------------


class CompareFilter:
    """State seeded by the controller's init values and then only read.

    There is nothing to journal, so the tasklet declares no change fold.
    """

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str | None:
        return value if ctx.state["comparable"] == value else None

    def initial_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"comparable": "default", "inverted": False}


compare_filter = flezha.tasklet(CompareFilter, name="compare_filter_tasklet")


@flezha.unbounded_operator("compare_filter")
def compare_filter_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(compare_filter).sink("sink")


# -- static_cell / sugar_static_cell / mixed_operator -------------------

STATIC_KEYS = ["a", "b", "c", "d"]


class SeenLog:
    """The reference's `seen: Vec<String>` state and its append-only fold.

    Four fixtures declare exactly this (the static entry, the sinkless short
    form, the partition relay and the dynamic cell) and differ only in what
    they do after recording.
    Sharing it is how the Ruby app spells the same thing.
    """

    def initial_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"seen": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["seen"].append(change["push"])


class StaticEntry(SeenLog):
    """The static fan-out every static fixture shares."""

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str:
        ctx.change({"push": value})
        return value

    def cells_for(self, value: str) -> list[str]:
        first = value[0] if value else ""
        return [first if first in STATIC_KEYS else "d"]


static_entry = flezha.tasklet(StaticEntry, name="static_entry_tasklet", cell_keys=STATIC_KEYS)
macro_static_entry = flezha.tasklet(
    StaticEntry, name="macro_static_entry_tasklet", cell_keys=STATIC_KEYS
)


static_downstream = passthrough("static_downstream_tasklet")


macro_static_downstream = passthrough("macro_static_downstream_tasklet")


@flezha.unbounded_operator("static_cell")
def static_cell_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(static_entry).named("hop").into(static_downstream).sink(
        "kafka"
    )


@flezha.unbounded_operator("sugar_static_cell")
def sugar_static_cell_operator(c: flezha.Channels) -> None:
    """The reference pipeline sugar leaves the hop unnamed and lets the sink
    name the producing channel."""
    c.input("input", codec="text").into(macro_static_entry).into(macro_static_downstream).sink(
        "test_kafka"
    )


mixed_macro_entry = passthrough("mixed_macro_entry_tasklet")


mixed_hand_downstream = passthrough("mixed_hand_downstream")


@flezha.unbounded_operator("mixed_operator")
def mixed_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(mixed_macro_entry).named("hop").into(
        mixed_hand_downstream
    ).sink("kafka")


# -- the §5 short forms ------------------------------------------------------
#
# One operator declaration each, carrying a tasklet body instead of a
# channels body. No hand-written operator or tasklet stands beside them.


@flezha.unbounded_operator(id="sugar_do_nothing", sink="sugar_do_nothing_echo", codec="text")
def sugar_do_nothing(value: str) -> str:
    return value


@flezha.unbounded_operator(id="sugar_store_input", sink="sugar_store_sink", codec="text")
class SugarStoreInput:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str:
        ctx.change({"push": value})
        return value

    def initial_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"text_inputs": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["text_inputs"].append(change["push"])


@flezha.unbounded_operator(
    id="sugar_named_input", sink="sugar_named_input_sink", input="prices", codec="text"
)
def sugar_named_input(value: str) -> str:
    return value


@flezha.bounded_operator(
    id="sugar_bounded", sink="sugar_bounded_placed", input="order", codec="text"
)
def sugar_bounded(order: str) -> str:
    return order


@flezha.bounded_operator(
    id="sugar_bounded_per_input",
    sink="sugar_bounded_per_input_out",
    codec="text",
    loop_mode="per_input",
)
def sugar_bounded_per_input(value: str) -> str:
    return value


@flezha.unbounded_operator(id="sugar_state_only", input="ticks", codec="text")
class SugarStateOnly(SeenLog):
    """No sink: what it was sent is proven by its state alone."""

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        ctx.change({"push": value})


# -- hybrid_channel ----------------------------------------------------------
#
# One emit reaching two destinations: an inter-tasklet consumer and a plugin
# wire with its own serializer. The serializer refuses a sentinel value, so an
# user can observe end to end that a refusal is isolated to the emit rather
# than failing the item.

HYBRID_SENTINEL = 0xDEADBEEF


@flezha.tasklet(name="hybrid_producer_tasklet")
def hybrid_producer(value: str, ctx: flezha.Ctx[None]) -> None:
    try:
        parsed = int(value)
    except ValueError:
        parsed = 0
    number = parsed if 0 <= parsed <= 0xFFFFFFFF else 0
    try:
        ctx.emit(number)
    except Exception as error:  # noqa: BLE001 - the isolation is the fixture
        print(
            f"hybrid_producer_tasklet: serializer refused an emit: {error}",
            file=sys.stderr,
            flush=True,
        )


@flezha.tasklet(name="hybrid_consumer_tasklet")
def hybrid_consumer(value: int) -> None:
    print(f"hybrid_consumer_tasklet: received {value}", flush=True)


def hybrid_encode(value: int) -> bytes:
    if value == HYBRID_SENTINEL:
        raise ValueError("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF")
    return struct.pack(">I", value)


@flezha.unbounded_operator("hybrid_channel")
def hybrid_channel_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(hybrid_producer).branch(
        # The consumer edge first, so a refused plugin serialization leaves the
        # in-process fan-out already recorded — the reference's isolation.
        lambda edge: edge.named("hybrid_indicator").into(hybrid_consumer),
        lambda edge: edge.sink(encode=hybrid_encode),
    )


# -- merged_input ------------------------------------------------------------
#
# Two entry channels merged into one tasklet. Each source's mapping tags the
# value with the channel it came from, the merge keys on the symbol the mapping
# read, and the emitted line is `<source>:<payload>`.


def tagged(source: str) -> Callable[[str], dict[str, str]]:
    def tag(payload: str) -> dict[str, str]:
        return {"source": source, "symbol": prefix(payload), "payload": payload}

    return tag


@flezha.tasklet(name="merge_label_tasklet", dynamic_cells=True)
def merge_label(value: dict[str, str]) -> str:
    return f"{value['source']}:{value['payload']}"


@flezha.unbounded_operator("merged_input")
def merged_input_operator(c: flezha.Channels) -> None:
    ticks = c.input("ticks", codec="text")
    quotes = c.input("quotes", codec="text")
    c.merge().from_(ticks, tagged("ticks")).from_(quotes, tagged("quotes")).key_by(
        lambda value: value["symbol"]
    ).into(merge_label).keyed().sink("merged")


# -- transform_chain ---------------------------------------------------------
#
# Every value transform on one edge: `drop` is filtered out, a comma list fans
# out into one value per element, each is upper-cased and keyed on its prefix.


transform_label = passthrough("transform_label_tasklet", dynamic_cells=True)


@flezha.unbounded_operator("transform_chain")
def transform_chain_operator(c: flezha.Channels) -> None:
    c.input("trades", codec="text").filter(lambda trade: trade != "drop").flat_map(
        lambda trade: trade.split(",")
    ).map(str.upper).key_by(prefix).into(transform_label).keyed().sink("transformed")


# -- cell_stress -----------------------------------------------------------

STRESS_KEYS = ["p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7"]


class StressFanOut:
    """Answers its input; the fan-out is which cells it names."""

    #: The cells every input reaches. `stress_a` fans out to all of them and
    #: the rest funnel back to one, which is the topology the stress exercises.
    targets: ClassVar[list[str]] = STRESS_KEYS

    def run(self, value: str) -> str:
        return value

    def cells_for(self, value: str) -> list[str]:
        return self.targets


class StressToP0(StressFanOut):
    targets: ClassVar[list[str]] = ["p0"]


stress_a = flezha.tasklet(StressFanOut, name="stress_a", cell_keys=STRESS_KEYS)
stress_b = flezha.tasklet(StressToP0, name="stress_b", cell_keys=STRESS_KEYS)
stress_c = flezha.tasklet(StressToP0, name="stress_c", cell_keys=STRESS_KEYS)
stress_d = flezha.tasklet(StressToP0, name="stress_d", cell_keys=STRESS_KEYS)


@flezha.unbounded_operator("cell_stress")
def cell_stress_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(stress_a).named("a_to_b").into(stress_b).named(
        "b_to_c"
    ).into(stress_c).named("c_to_d").into(stress_d).sink("echo")


# -- candle ------------------------------------------------------------------
#
# Late-input correction is about fold ORDER, and the only way to prove that is
# to look at the order. Send 100, 300, then the late 200. Without correction the
# fold order is [100, 300, 200] and the candle is permanently wrong. With it,
# the engine rewinds behind 200, replays the retained input log sorted by event
# time, and the fold order becomes [100, 200, 300].


class Candle:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        # `ctx.now()` IS the extracted event time under an event-time clock.
        event_time = ctx.now()
        ctx.change({"fold": event_time})
        # One row per candle bucket, so a corrected replay overwrites by key
        # rather than appending a second row.
        bucket = event_time // 1_000
        ctx.emit(f"{bucket}={value}", key=f"candle/{bucket}")

    def initial_state(self, cell_key: str | None) -> dict[str, Any]:
        #: Event times in fold order. The assertion surface.
        return {"folded": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["folded"].append(change["fold"])

    def event_time(self, value: str) -> int:
        return _as_int(value.split(":", 1)[0])


candle = flezha.tasklet(Candle, name="candle_tasklet", clock="event_time")


@flezha.unbounded_operator("candle")
def candle_operator(c: flezha.Channels) -> None:
    c.effectively_once().correction_window(600_000)
    c.input("input", codec="text").into(candle).keyed().sink("sink")


# -- poison_probe / wedge_probe ----------------------------------------------

_ANTIDOTE = {"taken": False}


@flezha.tasklet(name="poison_probe_tasklet")
def poison_probe(value: str) -> str:
    if value == "antidote":
        _ANTIDOTE["taken"] = True
    if value == "poison" and not _ANTIDOTE["taken"]:
        raise ValueError("poison payload")
    return f"ok:{value}"


@flezha.unbounded_operator("poison_probe")
def poison_probe_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").retry_backoff_ms(25).into(poison_probe).sink("out")


_ABORTED = {"done": False}


@flezha.tasklet(name="wedge_probe_tasklet")
def wedge_probe(value: str) -> str:
    """The ``pool_wedge`` fixture: the two ways a runner stops doing work.

    ``wedge`` blocks the dispatch inside user code forever, so the SDK's
    dispatch counter stops advancing while the heartbeat keeps reporting the
    runner alive: connected, heartbeating, processing nothing. ``abort`` ends
    its activation as violently as this host allows, WITHOUT emitting, once per
    process — CPython has no way for user code to kill a worker outright (the
    dispatcher catches per item and the raise rides the retry ladder), so what
    the scenario asserts is that the Cell is served again and the aborting
    item does not produce twice.
    """
    if value == "wedge":
        while True:
            time.sleep(3600)
    if value == "abort" and not _ABORTED["done"]:
        _ABORTED["done"] = True
        raise RuntimeError("wedge_probe: killing the worker holding this activation")
    return f"ok:{value}"


@flezha.unbounded_operator("wedge_probe")
def wedge_probe_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(wedge_probe).sink("out")


# -- session_timeout ---------------------------------------------------------
#
# The worked session-timeout shape (`cell_timers`): each input pushes the idle
# deadline out, and the firing closes the session. Single-cell so the e2e's
# crash-resume half exercises a durable Cell. The timeout is read from
# `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).


class SessionTimeout:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        ctx.change({"touched": value})
        # A processing-time timer, because the answer to "nothing has arrived"
        # cannot be an event-time one: nothing would advance the clock.
        ctx.timer_after(_env_int("TIMER_TEST_TIMEOUT_MS", 1_000) or 1_000)

    def on_timer(self, timer: dict[str, Any], ctx: flezha.Ctx[dict[str, Any]]) -> bytes:
        fired_at = int(timer["scheduled_at"])
        ctx.change({"closed": fired_at})
        return f"closed@{fired_at}".encode()

    def initial_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"touched": [], "closed_at": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        if "touched" in change:
            state["touched"].append(change["touched"])
        else:
            state["closed_at"].append(change["closed"])


session_timeout = flezha.tasklet(SessionTimeout, name="session_timeout_tasklet")


@flezha.unbounded_operator("session_timeout")
def session_timeout_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(session_timeout).sink("kafka")


# -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} ------------------


class BoundedMailbox:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str:
        ctx.change({"processed": True})
        return value

    def initial_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"processed": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["processed"] += 1


def _bounded_mailbox(policy: str) -> flezha.Operator:
    """One operator per overflow policy, over the same bounded-mailbox tasklet."""
    tasklet = flezha.tasklet(BoundedMailbox, name=f"{policy}_test_tasklet", mailbox_capacity=4)

    @flezha.unbounded_operator(f"bounded_mailbox_{policy}")
    def operator(c: flezha.Channels) -> None:
        c.input(f"{policy}_input", codec="text").on_full(policy).into(tasklet).sink(
            f"{policy}_echo"
        )

    return operator


BOUNDED_MAILBOX = [
    _bounded_mailbox(policy) for policy in ("drop_nothing", "drop_newest", "drop_oldest")
]


# -- partition_relay ---------------------------------------------------------
#
# The partitioned-operators upstream: keyed on the wiring so it can be
# partitioned at all, and emitting keyed so a partitioned forward target can
# resolve the owning member. The plugin channel is named `input` because a
# forwarded frame keeps its producing channel's name, and that is what selects
# the receiving operator's entry channel — so the entry here is `in`.


class PartitionRelay(SeenLog):
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> None:
        ctx.change({"push": value})
        ctx.emit(value, key=prefix(value))


partition_relay = flezha.tasklet(PartitionRelay, name="partition_relay_tasklet", dynamic_cells=True)


@flezha.unbounded_operator("partition_relay")
def partition_relay_operator(c: flezha.Channels) -> None:
    c.input("in", codec="text").key_by(prefix).into(partition_relay).keyed().sink("input")


# -- dynamic_cell ----------------------------------------------------------


class DynamicCell(SeenLog):
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any]]) -> str:
        ctx.change({"push": value})
        return value


dynamic_cell = flezha.tasklet(
    DynamicCell,
    name="dynamic_cell_tasklet",
    dynamic_cells=True,
    idle_ttl_ms=_env_int("DYN_TEST_TTL_MS", None),
)


@flezha.unbounded_operator("dynamic_cell")
def dynamic_cell_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").on_full("drop_nothing").filter(
        # Filtered before keying: a dropped value answers no key, so the core
        # never mints a cell for it.
        lambda value: value != "drop"
    ).key_by(prefix).into(dynamic_cell).sink("kafka")


# -- spawn_requester ---------------------------------------------------------
#
# On each input it spawns `stateless_loop_bounded` once with no id and once
# under the input as its id, and prints both answers, because a handle's answer
# is observable nowhere but in the tasklet that read it.


def _report(label: str, handle: Any) -> None:
    """A spawn handle's answer on stdout, as `spawn_result <label> <id> <outcome>`."""
    try:
        result = handle.wait()
        print(f"spawn_result {label} {result.id} {result.outcome}", flush=True)
    except flezha.SpawnError as error:
        print(f"spawn_result {label} error {error}", flush=True)


@flezha.tasklet(name="spawn_requester")
def spawn_requester(value: str, ctx: flezha.Ctx[None]) -> None:
    factory = stateless_loop_bounded.factory_id
    handles = [
        ("unnamed", ctx.spawn_bounded(factory, value)),
        ("named", ctx.spawn_bounded(factory, value, id=value)),
    ]
    for label, handle in handles:
        _report(label, handle)


@flezha.unbounded_operator("spawn_requester")
def spawn_requester_operator(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(spawn_requester).sink("out")


# -- spawn_once_requester ----------------------------------------------------
#
# On each input it spawns `typed_once_bounded` once, with no id, carrying the
# input inside a JSON object its `order` entry decodes, and prints the result
# as `spawn_result once <id> <outcome>`: the payload a spawn carries is the
# spawned instance's first message, encoded by the no-codec sink rule.


@flezha.tasklet(name="spawn_once_requester")
def spawn_once_requester(value: str, ctx: flezha.Ctx[None]) -> None:
    _report("once", ctx.spawn_bounded("typed_once_bounded", {"text": value}))


@flezha.unbounded_operator("spawn_once_requester")
def spawn_once_requester_operator(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(spawn_once_requester).sink("out")


#: Every operator this app registers, in declaration order.
ALL: list[flezha.Operator] = [
    do_nothing_operator,
    bench_echo_operator,
    compare_filter_operator,
    static_cell_operator,
    sugar_static_cell_operator,
    mixed_operator,
    sugar_do_nothing,
    SugarStoreInput,
    sugar_named_input,
    sugar_bounded,
    sugar_bounded_per_input,
    SugarStateOnly,
    hybrid_channel_operator,
    merged_input_operator,
    transform_chain_operator,
    cell_stress_operator,
    bench_keyed_nopersist_operator,
    store_input_operator,
    stateful_counter_operator,
    persistence_off_operator,
    sleeping_counter_operator,
    coalescing_counter_operator,
    resume_probe_operator,
    bench_keyed_operator,
    schema_evolution_v1_operator,
    schema_evolution_v2_operator,
    tier_store_operator,
    checkpoint_store_operator,
    checkpoint_batched_operator,
    bench_checkpoint_operator,
    candle_operator,
    poison_probe_operator,
    wedge_probe_operator,
    session_timeout_operator,
    *BOUNDED_MAILBOX,
    partition_relay_operator,
    dynamic_cell_operator,
    spawn_requester_operator,
    spawn_once_requester_operator,
]
