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
from fixture_support import FIXTURE_DURABLE, _as_int, _env_int, passthrough, prefix
from fixtures_state import (
    bench_checkpoint_relay,
    bench_keyed_nopersist_relay,
    bench_keyed_relay,
    checkpoint_batched_relay,
    checkpoint_store_relay,
    coalescing_counter_relay,
    persistence_off_relay,
    remote_tier_store_relay,
    resume_probe_relay,
    schema_evolution_v1_relay,
    schema_evolution_v2_relay,
    sleeping_counter_relay,
    stateful_counter_relay,
    store_input_relay,
    tier_chain_relay,
    tier_store_relay,
)
from fixtures_steps import (
    LOOP_KNOBS,
    block_until_released,
    stateless_loop_steps,
    stateless_step,
)

# -- do_nothing / bench_echo -------------------------------------------------

do_nothing = passthrough("do_nothing_tasklet")


@flezha.relay("do_nothing")
def do_nothing_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(do_nothing).sink("echo")


@flezha.relay("bench_echo")
def bench_echo_relay(c: flezha.Channels) -> None:
    """Echo with a declared DropNothing on its entry mailbox.

    ``do_nothing`` is the merge-blocking bench factory and must not change; this
    keeps the round-trip bench's back-pressure posture independent of it.
    """
    c.input("input", codec="text").on_full("drop_nothing").into(do_nothing).sink("echo")


# -- complete_flow / chained_steps -------------------------------------------


def _complete_on_bye(value: str, ctx: flezha.Ctx[None, str]) -> None:
    if value == "bye":
        ctx.complete(value)
    ctx.emit(value)


@flezha.relay("complete_flow")
def complete_flow_relay(c: flezha.Channels) -> None:
    """A flow class that echoes each input and ends its instance on ``bye``."""
    tasklet = stateless_step("complete_flow_tasklet", _complete_on_bye)
    c.input("input", codec="text").into(tasklet).sink("complete_flow_out")


def _count_down(value: str, ctx: flezha.Ctx[None, str]) -> None:
    print(f"chained_pass {value}", flush=True)
    if int(value) > 0:
        ctx.emit(str(int(value) - 1))


@flezha.relay("chained_steps", run={"steps": {"each": "chained"}})
def chained_steps_relay(c: flezha.Channels) -> None:
    """A durable chained loop: each pass hands the next ``n - 1``; zero ends it."""
    countdown = stateless_step("chained_countdown", _count_down)
    c.input("in", codec="text").into(countdown).named("carry")


# -- compare_filter ----------------------------------------------------------


class CompareFilter:
    """State seeded by the controller's init values and then only read.

    There is nothing to journal, so the tasklet declares no change fold.
    """

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], str]) -> None:
        if ctx.state["comparable"] == value:
            ctx.emit(value)

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"comparable": "default", "inverted": False}


compare_filter = flezha.tasklet(CompareFilter, name="compare_filter_tasklet")


@flezha.relay("compare_filter")
def compare_filter_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(compare_filter).sink("sink")


# -- static_cell / sugar_static_cell / mixed_relay -------------------

STATIC_KEYS = ["a", "b", "c", "d"]


class SeenLog:
    """The reference's `seen: Vec<String>` state and its append-only fold.

    Four fixtures declare exactly this (the static entry, the sinkless short
    form, the partition pass and the dynamic cell) and differ only in what
    they do after recording.
    Sharing it is how the Ruby app spells the same thing.
    """

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"seen": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["seen"].append(change["push"])


class StaticEntry(SeenLog):
    """The static fan-out every static fixture shares."""

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], str]) -> None:
        ctx.change({"push": value})
        ctx.emit(value)

    def cells_for(self, value: str) -> list[str]:
        first = value[0] if value else ""
        return [first if first in STATIC_KEYS else "d"]


static_entry = flezha.tasklet(StaticEntry, name="static_entry_tasklet", cell_keys=STATIC_KEYS)
macro_static_entry = flezha.tasklet(
    StaticEntry, name="macro_static_entry_tasklet", cell_keys=STATIC_KEYS
)


static_downstream = passthrough("static_downstream_tasklet")


macro_static_downstream = passthrough("macro_static_downstream_tasklet")


@flezha.relay("static_cell")
def static_cell_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(static_entry).named("hop").into(static_downstream).sink(
        "kafka"
    )


@flezha.relay("sugar_static_cell")
def sugar_static_cell_relay(c: flezha.Channels) -> None:
    """The reference pipeline sugar leaves the hop unnamed and lets the sink
    name the producing channel."""
    c.input("input", codec="text").into(macro_static_entry).into(macro_static_downstream).sink(
        "test_kafka"
    )


mixed_macro_entry = passthrough("mixed_macro_entry_tasklet")


mixed_hand_downstream = passthrough("mixed_hand_downstream")


@flezha.relay("mixed_relay")
def mixed_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(mixed_macro_entry).named("hop").into(
        mixed_hand_downstream
    ).sink("kafka")


# -- the §5 short forms ------------------------------------------------------
#
# One relay declaration each, carrying a tasklet body instead of a
# channels body. No hand-written relay or tasklet stands beside them.


@flezha.relay(id="sugar_do_nothing", sink="sugar_do_nothing_echo", codec="text")
def sugar_do_nothing(value: str) -> str:
    return value


@flezha.relay(id="sugar_store_input", sink="sugar_store_sink", codec="text")
class SugarStoreInput:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], str]) -> None:
        ctx.change({"push": value})
        ctx.emit(value)

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"text_inputs": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["text_inputs"].append(change["push"])


@flezha.relay(
    id="sugar_named_input",
    sink="sugar_named_input_sink",
    input="prices",
    codec="text",
)
def sugar_named_input(value: str) -> str:
    return value


@flezha.relay(
    id="sugar_steps",
    run="steps",
    durable=FIXTURE_DURABLE,
    sink="sugar_steps_placed",
    input="order",
    codec="text",
)
def sugar_steps(order: str) -> str:
    return order


@flezha.relay(
    id="sugar_steps_per_input",
    run={"steps": {"each": "per_input"}},
    durable=FIXTURE_DURABLE,
    sink="sugar_steps_per_input_out",
    codec="text",
)
def sugar_steps_per_input(value: str) -> str:
    return value


@flezha.relay(id="sugar_state_only", input="ticks", codec="text")
class SugarStateOnly(SeenLog):
    """No sink: what it was sent is proven by its state alone."""

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], None]) -> None:
        ctx.change({"push": value})


# -- hybrid_channel ----------------------------------------------------------
#
# One emit reaching two destinations: an inter-tasklet consumer and a plugin
# wire with its own serializer. The serializer refuses a sentinel value, so an
# user can observe end to end that a refusal is isolated to the emit rather
# than failing the item.

HYBRID_SENTINEL = 0xDEADBEEF


@flezha.tasklet(name="hybrid_producer_tasklet")
class HybridProducer:
    def run(self, value: str, ctx: flezha.Ctx[None, Any]) -> None:
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
class HybridConsumer:
    def run(self, value: int, ctx: flezha.Ctx[None, None]) -> None:
        print(f"hybrid_consumer_tasklet: received {value}", flush=True)


def hybrid_encode(value: int) -> bytes:
    if value == HYBRID_SENTINEL:
        raise ValueError("hybrid_indicators: serializer rejected sentinel 0xDEADBEEF")
    return struct.pack(">I", value)


@flezha.relay("hybrid_channel")
def hybrid_channel_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(HybridProducer).branch(
        # The consumer edge first, so a refused plugin serialization leaves the
        # in-process fan-out already recorded — the reference's isolation.
        lambda edge: edge.named("hybrid_indicator").into(HybridConsumer),
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


@flezha.tasklet(name="merge_label_tasklet")
class MergeLabel:
    def run(self, value: dict[str, str], ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(f"{value['source']}:{value['payload']}")


@flezha.relay("merged_input")
def merged_input_relay(c: flezha.Channels) -> None:
    ticks = c.input("ticks", codec="text")
    quotes = c.input("quotes", codec="text")
    c.merge().from_(ticks, tagged("ticks")).from_(quotes, tagged("quotes")).key_by(
        lambda value: value["symbol"]
    ).into(MergeLabel).keyed().sink("merged")


# -- transform_chain ---------------------------------------------------------
#
# Every value transform on one edge: `drop` is filtered out, a comma list fans
# out into one value per element, each is upper-cased and keyed on its prefix.


transform_label = passthrough("transform_label_tasklet")


@flezha.relay("transform_chain")
def transform_chain_relay(c: flezha.Channels) -> None:
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

    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)

    def cells_for(self, value: str) -> list[str]:
        return self.targets


class StressToP0(StressFanOut):
    targets: ClassVar[list[str]] = ["p0"]


stress_a = flezha.tasklet(StressFanOut, name="stress_a", cell_keys=STRESS_KEYS)
stress_b = flezha.tasklet(StressToP0, name="stress_b", cell_keys=STRESS_KEYS)
stress_c = flezha.tasklet(StressToP0, name="stress_c", cell_keys=STRESS_KEYS)
stress_d = flezha.tasklet(StressToP0, name="stress_d", cell_keys=STRESS_KEYS)


@flezha.relay("cell_stress")
def cell_stress_relay(c: flezha.Channels) -> None:
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
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        # `ctx.now()` IS the extracted event time under an event-time clock.
        event_time = ctx.now()
        ctx.change({"fold": event_time})
        # One row per candle bucket, so a corrected replay overwrites by key
        # rather than appending a second row.
        bucket = event_time // 1_000
        ctx.emit(f"{bucket}={value}", key=f"candle/{bucket}")

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        #: Event times in fold order. The assertion surface.
        return {"folded": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["folded"].append(change["fold"])

    def event_time(self, value: str) -> int:
        return _as_int(value.split(":", 1)[0])


candle = flezha.tasklet(Candle, name="candle_tasklet")


@flezha.relay("candle", run={"flow": {"exactly": {"effectively_once": {"correction_ms": 600_000}}}})
def candle_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(candle).keyed().sink("sink")


# -- poison_probe / wedge_probe ----------------------------------------------

_ANTIDOTE = {"taken": False}


@flezha.tasklet(name="poison_probe_tasklet")
class PoisonProbe:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        if value == "antidote":
            _ANTIDOTE["taken"] = True
        if value == "poison" and not _ANTIDOTE["taken"]:
            raise ValueError("poison payload")
        ctx.emit(f"ok:{value}")


@flezha.relay("poison_probe")
def poison_probe_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").retry_backoff_ms(25).into(PoisonProbe).sink("out")


_ABORTED = {"done": False}


@flezha.tasklet(name="wedge_probe_tasklet")
class WedgeProbe:
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

    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        if value == "wedge":
            while True:
                time.sleep(3600)
        if value == "abort" and not _ABORTED["done"]:
            _ABORTED["done"] = True
            raise RuntimeError("wedge_probe: killing the worker holding this activation")
        ctx.emit(f"ok:{value}")


@flezha.relay("wedge_probe")
def wedge_probe_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(WedgeProbe).sink("out")


# -- session_timeout ---------------------------------------------------------
#
# The worked session-timeout shape (`cell_timers`): each input pushes the idle
# deadline out, and the firing closes the session. Single-cell so the e2e's
# crash-resume half exercises a durable Cell. The timeout is read from
# `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).


class SessionTimeout:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], bytes]) -> None:
        ctx.change({"touched": value})
        # A processing-time timer, because the answer to "nothing has arrived"
        # cannot be an event-time one: nothing would advance the clock.
        ctx.timer_after(_env_int("TIMER_TEST_TIMEOUT_MS", 1_000) or 1_000)

    def on_timer(self, timer: dict[str, Any], ctx: flezha.Ctx[dict[str, Any], bytes]) -> None:
        fired_at = int(timer["scheduled_at"])
        ctx.change({"closed": fired_at})
        ctx.emit(f"closed@{fired_at}".encode())

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"touched": [], "closed_at": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        if "touched" in change:
            state["touched"].append(change["touched"])
        else:
            state["closed_at"].append(change["closed"])


session_timeout = flezha.tasklet(SessionTimeout, name="session_timeout_tasklet")


@flezha.relay("session_timeout")
def session_timeout_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(session_timeout).sink("kafka")


# -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} ------------------


class BoundedMailbox:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], str]) -> None:
        ctx.change({"processed": True})
        ctx.emit(value)

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"processed": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["processed"] += 1


def _bounded_mailbox(policy: str) -> flezha.Relay:
    """One relay per overflow policy, over the same bounded-mailbox tasklet."""
    tasklet = flezha.tasklet(BoundedMailbox, name=f"{policy}_test_tasklet", mailbox_capacity=4)

    @flezha.relay(f"bounded_mailbox_{policy}")
    def relay(c: flezha.Channels) -> None:
        c.input(f"{policy}_input", codec="text").on_full(policy).into(tasklet).sink(
            f"{policy}_echo"
        )

    return relay


BOUNDED_MAILBOX = [
    _bounded_mailbox(policy) for policy in ("drop_nothing", "drop_newest", "drop_oldest")
]


# -- partition_pass ---------------------------------------------------------
#
# The partitioned-relays upstream: keyed on the wiring so it can be
# partitioned at all, and emitting keyed so a partitioned forward target can
# resolve the owning member. The plugin channel is named `input` because a
# forwarded frame keeps its producing channel's name, and that is what selects
# the receiving relay's entry channel — so the entry here is `in`.


class PartitionPass(SeenLog):
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        ctx.change({"push": value})
        ctx.emit(value, key=prefix(value))


partition_pass = flezha.tasklet(PartitionPass, name="partition_pass_tasklet")


@flezha.relay("partition_pass")
def partition_pass_relay(c: flezha.Channels) -> None:
    c.input("in", codec="text").key_by(prefix).into(partition_pass).keyed().sink("input")


# -- dynamic_cell ----------------------------------------------------------


class DynamicCell(SeenLog):
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], str]) -> None:
        ctx.change({"push": value})
        ctx.emit(value)


#: The e2e sets the TTL in milliseconds for every SDK's runner; an idle TTL is
#: whole seconds, so it is rounded up.
_DYN_TTL_MS = _env_int("DYN_TEST_TTL_MS", None)
dynamic_cell = flezha.tasklet(
    DynamicCell,
    name="dynamic_cell_tasklet",
    idle_ttl_seconds=None if _DYN_TTL_MS is None else -(-_DYN_TTL_MS // 1_000),
)


@flezha.relay("dynamic_cell")
def dynamic_cell_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").on_full("drop_nothing").filter(
        # Filtered before keying: a dropped value answers no key, so the core
        # never mints a cell for it.
        lambda value: value != "drop"
    ).key_by(prefix).into(dynamic_cell).sink("kafka")


# -- spawn_requester ---------------------------------------------------------
#
# On each input it spawns `stateless_loop_steps` once with no id and once
# under the input as its id, and prints both answers, because a handle's answer
# is observable nowhere but in the tasklet that read it.


def _report(label: str, handle: Any) -> None:
    """A spawn handle's answer on stdout, as `spawn_result <label> <id> <outcome>`."""
    try:
        result = handle.wait()
        print(f"spawn_result {label} {result.id} {result.outcome}", flush=True)
    except flezha.FlezhaError as error:
        print(f"spawn_result {label} error {error.code}", flush=True)


@flezha.tasklet(name="spawn_requester")
class SpawnRequester:
    def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
        factory = stateless_loop_steps.factory_id
        handles = [
            ("unnamed", ctx.spawn_steps(factory, value)),
            ("named", ctx.spawn_steps(factory, value, id=value)),
        ]
        for label, handle in handles:
            _report(label, handle)


@flezha.relay("spawn_requester")
def spawn_requester_relay(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(SpawnRequester).sink("out")


# -- spawn_hold_requester ----------------------------------------------------
#
# An effectively-once stream whose tasklet spawns `stateless_loop_steps` with
# no id, prints the answer as `spawn_result hold <id> <outcome>`, then blocks
# until the release env is set, so a runner killed there dies with the spawn
# answered and its input not durably consumed.


@flezha.tasklet(name="spawn_hold_requester")
class SpawnHoldRequester:
    async def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
        _report("hold", ctx.spawn_steps(stateless_loop_steps.factory_id, value))
        await block_until_released()


@flezha.relay("spawn_hold_requester", run={"flow": {"exactly": "effectively_once"}})
def spawn_hold_requester_relay(c: flezha.Channels) -> None:
    c.input("in", codec="text").on_full("drop_nothing").into(SpawnHoldRequester).keyed().sink("out")


# -- spawn_once_requester ----------------------------------------------------
#
# On each input it spawns `typed_once_steps` once, with no id, carrying the
# input inside a JSON object its `order` entry decodes, and prints the result
# as `spawn_result once <id> <outcome>`: the payload a spawn carries is the
# spawned instance's first message, encoded by the no-codec sink rule.


@flezha.tasklet(name="spawn_once_requester")
class SpawnOnceRequester:
    def run(self, value: str, ctx: flezha.Ctx[None, None]) -> None:
        _report("once", ctx.spawn_steps("typed_once_steps", {"text": value}))


@flezha.relay("spawn_once_requester")
def spawn_once_requester_relay(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(SpawnOnceRequester).sink("out")


# -- call_response -----------------------------------------------------------
#
# Relays an outside caller calls and whose sink output answers it: from the
# first tasklet, from a downstream one, and from whichever of two filtered
# paths the input took. `call_turn` is the steps
# one, in fixtures_steps.py.


@flezha.relay("call_echo")
def call_echo_relay(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(passthrough("call_echo")).sink()


call_answer = stateless_step("call_answer", lambda value, ctx: ctx.emit(f"re:{value}"))


@flezha.relay("call_downstream")
def call_downstream_relay(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(passthrough("call_parse")).into(call_answer).sink()


# An input starting `no` takes the refused path, every other the accepted one.
call_accept = stateless_step("call_accept", lambda value, ctx: ctx.emit(f"accepted:{value}"))
call_refuse = stateless_step("call_refuse", lambda value, ctx: ctx.emit(f"refused:{value}"))


@flezha.relay("call_split")
def call_split_relay(c: flezha.Channels) -> None:
    admitted = c.input("in", codec="text").into(passthrough("call_admit"))
    admitted.filter(lambda value: not value.startswith("no")).into(call_accept).sink()
    admitted.filter(lambda value: value.startswith("no")).into(call_refuse).sink()


# -- parts ----------------------------------------------------------------
#
# For an input `n` the tasklet sends the parts `1` .. `n` and then emits
# `done:<n>` on its sink `answer`, in a flow relay and in a
# `per_input` steps one. `retry:<n>` sends its parts and then fails
# retryably the first time this process sees it, so the retry's parts number
# from 0 again under the next attempt.

_FAILED_ONCE: set[str] = set()


def _send_parts(value: str, ctx: flezha.Ctx[None, str]) -> None:
    count = value.removeprefix("retry:")
    if not count.isdigit():
        raise flezha.NonRetryableError(f"not a part count: {value}")
    for part in range(1, int(count) + 1):
        ctx.part(str(part))
    if value.startswith("retry:") and value not in _FAILED_ONCE:
        _FAILED_ONCE.add(value)
        raise RuntimeError(f"first attempt of {value}")
    ctx.emit(f"done:{count}")


@flezha.relay("part_echo")
def part_echo_relay(c: flezha.Channels) -> None:
    c.input("in", codec="text").retry_backoff_ms(25).into(
        stateless_step("part_echo", _send_parts)
    ).sink("answer")


@flezha.relay("part_echo_steps", **LOOP_KNOBS)
def part_echo_steps(c: flezha.Channels) -> None:
    c.input("in", codec="text").retry_backoff_ms(25).into(
        stateless_step("part_echo_turn", _send_parts)
    ).sink("answer")


#: Every relay this app registers, in declaration order.
ALL: list[flezha.Relay] = [
    do_nothing_relay,
    complete_flow_relay,
    chained_steps_relay,
    bench_echo_relay,
    compare_filter_relay,
    static_cell_relay,
    sugar_static_cell_relay,
    mixed_relay,
    sugar_do_nothing,
    SugarStoreInput,
    sugar_named_input,
    sugar_steps,
    sugar_steps_per_input,
    SugarStateOnly,
    hybrid_channel_relay,
    merged_input_relay,
    transform_chain_relay,
    cell_stress_relay,
    bench_keyed_nopersist_relay,
    store_input_relay,
    stateful_counter_relay,
    persistence_off_relay,
    sleeping_counter_relay,
    coalescing_counter_relay,
    resume_probe_relay,
    bench_keyed_relay,
    schema_evolution_v1_relay,
    schema_evolution_v2_relay,
    tier_store_relay,
    remote_tier_store_relay,
    tier_chain_relay,
    checkpoint_store_relay,
    checkpoint_batched_relay,
    bench_checkpoint_relay,
    candle_relay,
    poison_probe_relay,
    wedge_probe_relay,
    session_timeout_relay,
    *BOUNDED_MAILBOX,
    partition_pass_relay,
    dynamic_cell_relay,
    spawn_requester_relay,
    spawn_hold_requester_relay,
    spawn_once_requester_relay,
    call_echo_relay,
    call_downstream_relay,
    call_split_relay,
    part_echo_relay,
    part_echo_steps,
]
