"""Fixtures whose subject is persisted Cell state: the keyed upsert the
bench and tier classes share, the counter and resume twins, schema evolution,
the effectively-once tier store with its checkpoint variants, and the Cell
timers.
"""

from __future__ import annotations

import os
from typing import Any

import flezha
from fixture_support import FIXTURE_DURABLE, _as_int, _env_int, prefix
from fixtures_steps import RELEASE_ENV
from flezha import codec as flezha_codec

# -- bench_keyed_nopersist ---------------------------------------------------


class KeyedUpsert:
    """The reference's `TierStoreState` and its upsert fold.

    `tier_store`, `checkpoint_store` and `bench_checkpoint` are the same keyed
    upsert under three acknowledgement postures; only `run` differs.
    """

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"values": {}}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["values"][change["key"]] = change["value"]

    def upsert(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        """Record and emit one `key:value` input under its natural key."""
        key, _, item = value.partition(":")
        ctx.change({"key": key, "value": item})
        ctx.emit(f"{key}={item}", key=key)


class TierStore(KeyedUpsert):
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], None]) -> None:
        if value.partition(":")[2] == "poison":
            raise ValueError("poison value")
        self.upsert(value, ctx)


tier_store = flezha.tasklet(TierStore, name="tier_store_tasklet")


@flezha.relay("bench_keyed_nopersist", durable=False)
def bench_keyed_nopersist_relay(c: flezha.Channels) -> None:
    """Per-cell persistence off: the heap-only posture the bench isolates
    against the durable one."""
    c.input("input", codec="text").on_full("drop_nothing").into(tier_store).keyed().output("sink")


# -- store_input / stateful_counter / coalescing_counter / persistence_off ---
# -- resume_probe / bench_keyed ----------------------------------------------


class StoreInput:
    """The durable-state reference: every input is appended through a change."""

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], str]) -> None:
        ctx.change({"push_text": value})
        ctx.emit(value)

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"binary_inputs": [], "text_inputs": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["text_inputs"].append(change["push_text"])


store_input = flezha.tasklet(StoreInput, name="store_input_tasklet")


@flezha.relay("store_input")
def store_input_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(store_input).output("sink")


@flezha.tasklet(name="counter_entry_tasklet")
class CounterEntry:
    def run(self, value: str, ctx: flezha.Ctx[None, int]) -> None:
        ctx.emit(_as_int(value))


class Counter:
    def run(self, value: int, ctx: flezha.Ctx[dict[str, Any], None]) -> None:
        ctx.change({"delta": value})

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"value": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["value"] += change["delta"]


counter = flezha.tasklet(Counter, name="counter_tasklet")


@flezha.relay("stateful_counter")
def stateful_counter_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(CounterEntry).named("ticks").into(counter)


@flezha.relay("persistence_off_counter", durable=False)
def persistence_off_relay(c: flezha.Channels) -> None:
    """The same topology with per-Cell persistence off.

    Its own node names, as the reference declares them: a node identity is per
    class, so reusing `stateful_counter`'s would leave the two classes
    indistinguishable to anything that reads a declaration.
    """
    c.input("input", codec="text").into(CounterEntry, "persistence_off_entry_tasklet").named(
        "ticks"
    ).into(counter, "persistence_off_counter_tasklet")


# -- sleeping_counter --------------------------------------------------------
#
# A flow relay that sleeps when idle: `key:delta` inputs fold into one running
# total per key, and every input emits `key=total` on the `totals` output. The
# sleep e2e reads the total after a wake to prove the Cell came back with the
# state it slept with.


class SleepingCounter:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], str]) -> None:
        key, _, delta = value.partition(":")
        ctx.change({"delta": _as_int(delta)})
        ctx.emit(f"{key}={ctx.state['value']}")

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"value": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["value"] += change["delta"]


sleeping_counter = flezha.tasklet(SleepingCounter, name="sleeping_counter_tasklet")


@flezha.relay("sleeping_counter", durable=FIXTURE_DURABLE)
def sleeping_counter_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").key_by(prefix).into(sleeping_counter).output("totals")


@flezha.tasklet(name="tick_entry_tasklet")
class TickEntry:
    """Wire format: ``<interval>:<ts>:<delta>``."""

    def run(self, value: str, ctx: flezha.Ctx[None, dict[str, int]]) -> None:
        parts = value.split(":", 2)

        def number(index: int) -> int:
            return _as_int(parts[index]) if index < len(parts) else 0

        ctx.emit({"interval": number(0), "ts": number(1), "delta": number(2)})


class Tick:
    """A change whose coalesce key is `(interval, ts)`.

    The SDK carries no coalesce-key seam — the Rust host's is a `Change` trait
    method — so the change ships uncoalesced, which is a throughput property
    and not a semantic one: the fold is idempotent per `(interval, ts)` either
    way, so replay produces the same state.
    """

    def run(self, value: dict[str, int], ctx: flezha.Ctx[dict[str, Any], None]) -> None:
        ctx.change(dict(value))

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"last": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["last"] = change["delta"]


tick = flezha.tasklet(Tick, name="tick_tasklet")


@flezha.relay("coalescing_counter")
def coalescing_counter_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(TickEntry).named("ticks").into(tick)


class ResumeProbe:
    """Emits the running total, so a resume is observable on the wire."""

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], bytes]) -> None:
        delta = _as_int(value)
        total = ctx.state["total"] + delta
        ctx.change({"add": delta})
        ctx.emit(str(total).encode())

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        # Default + change replay rebuilds the total on restore (the same
        # pattern as stateful_counter); no snapshot fast path needed.
        return {"total": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["total"] += change["add"]


resume_probe = flezha.tasklet(ResumeProbe, name="resume_probe_tasklet")


@flezha.relay("resume_probe")
def resume_probe_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(resume_probe).output("total")


@flezha.relay("bench_keyed")
def bench_keyed_relay(c: flezha.Channels) -> None:
    """`bench_keyed_nopersist`'s durable twin: the same keyed upsert with
    per-Cell persistence on, so the bench can isolate that cost."""
    c.input("input", codec="text").on_full("drop_nothing").into(
        tier_store, "tier_store_tasklet"
    ).keyed().output("sink")


# -- schema_evolution_v1 / schema_evolution_v2 -------------------------------
#
# The same class either side of a deploy that edits its own state type. Both
# halves declare the SAME tasklet name, so a relay started under v1 and
# restarted under v2 restores from the very WAL v1 wrote — which is what a
# deploy does, with the two binaries collapsed into one fixture file.
#
# v1: `{total}`. v2: `{total, currency}` at schema version 2, reading down to 1
# through a declared migration. Output is `total:currency` so the e2e can read
# the migrated field off the wire.


class EvolvingCartV1:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], bytes]) -> None:
        delta = _as_int(value)
        total = ctx.state["total"] + delta
        ctx.change({"add": delta})
        ctx.emit(f"{total}:".encode())

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"total": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["total"] += change["add"]


class EvolvingCartV2:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], bytes]) -> None:
        delta = _as_int(value)
        total = ctx.state["total"] + delta
        currency = ctx.state.get("currency") or "EUR"
        ctx.change({"add": delta, "note": "live"})
        ctx.emit(f"{total}:{currency}".encode())

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"total": 0, "currency": ""}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["total"] += change["add"]

    def migrate_state(self, from_version: int, body: Any) -> dict[str, Any]:
        if from_version != 1:
            raise ValueError(f"no migration from {from_version}")
        old = flezha_codec.decode(body) or {}
        return {"total": old.get("total", 0), "currency": "EUR"}

    def migrate_change(self, from_version: int, body: Any) -> dict[str, Any]:
        if from_version != 1:
            raise ValueError(f"no migration from {from_version}")
        old = flezha_codec.decode(body) or {}
        return {"add": old.get("add", 0), "note": "migrated"}


evolving_cart_v1 = flezha.tasklet(EvolvingCartV1, name="evolving_cart")
evolving_cart_v2 = flezha.tasklet(
    EvolvingCartV2,
    name="evolving_cart",
    schema_version=2,
    min_readable_schema_version=1,
)


@flezha.relay("schema_evolution_v1")
def schema_evolution_v1_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(evolving_cart_v1).output("total")


@flezha.relay("schema_evolution_v2")
def schema_evolution_v2_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(evolving_cart_v2).output("total")


# -- tier_store / checkpoint_store / checkpoint_batched_store / bench_checkpoint


@flezha.relay("tier_store", run={"flow": {"exactly": "effectively_once"}})
def tier_store_relay(c: flezha.Channels) -> None:
    """The effectively-once reference: a keyed upsert under the tier.

    A 300 ms backoff base keeps the min-watermark freeze observable in the DLQ
    e2e. Declared DropNothing: a durable-input-log relay that shed at its
    mailbox would hold the min-watermark on the shed offset forever while the
    log behind it kept growing.
    """
    c.input("input", codec="text").on_full("drop_nothing").retry_backoff_ms(300).into(
        tier_store, "tier_store_tasklet"
    ).keyed().output("sink")


@flezha.relay("remote_tier_store", run={"flow": {"exactly": "effectively_once"}}, durable="remote")
def remote_tier_store_relay(c: flezha.Channels) -> None:
    """``tier_store`` kept ``durable: remote``: an input is acknowledged only once
    the durability backend holds the records it produced."""
    tier_store_relay.declaration(c)


# -- tier_chain --------------------------------------------------------------
#
# An effectively-once stream of two tasklets. `tier_pass` counts what it
# forwards and passes each `key:value` on; `tier_tally` counts each key's inputs
# and emits `key=count`. Until the release env is set the tally fails the value
# `hold` and keeps retrying it, so a runner killed there has made the forward's
# record durable and not the tally's.


class Counting:
    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"value": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["value"] += change["delta"]


class TierPass(Counting):
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        ctx.change({"delta": 1})
        ctx.emit(value)


class TierTally(Counting):
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        key, _, item = value.partition(":")
        if item == "hold" and RELEASE_ENV not in os.environ:
            raise ValueError("held until released")
        ctx.change({"delta": 1})
        ctx.emit(f"{key}={ctx.state['value']}", key=key)


tier_pass = flezha.tasklet(TierPass, name="tier_pass")
tier_tally = flezha.tasklet(TierTally, name="tier_tally")


@flezha.relay("tier_chain", run={"flow": {"exactly": "effectively_once"}})
def tier_chain_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").on_full("drop_nothing").into(tier_pass).key_by(prefix).retry_max(
        1_000
    ).into(tier_tally).keyed().output("sink")


class CheckpointStore(KeyedUpsert):
    """``tier_store``'s twin, whose interval closes on a `commit:` input."""

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], None]) -> None:
        if value.partition(":")[0] == "commit":
            ctx.checkpoint()
            return
        self.upsert(value, ctx)


checkpoint_store = flezha.tasklet(CheckpointStore, name="checkpoint_store_tasklet")


@flezha.relay(
    "checkpoint_store", run={"flow": {"exactly": {"effectively_once": {"ack": "checkpoint"}}}}
)
def checkpoint_store_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(checkpoint_store).keyed().output("sink")


@flezha.relay(
    "checkpoint_batched_store",
    run={
        "flow": {
            "exactly": {
                "effectively_once": {
                    "ack": {"checkpoint": {"outputs": "at_checkpoint", "max_delay_ms": 1_500}}
                }
            }
        }
    },
)
def checkpoint_batched_relay(c: flezha.Channels) -> None:
    """The same tasklet declared ``outputs: at_checkpoint``.

    The interval's outputs are held with its changes and ship inside its
    checkpoint record. ``every`` is deliberately absent so the test
    drives the close itself — with ``commit:`` for the batch case, and with the
    delay alone for the quiet-Cell case.
    """
    c.input("input", codec="text").into(
        checkpoint_store, "checkpoint_store_tasklet"
    ).keyed().output("sink")


class BenchCheckpoint(KeyedUpsert):
    """``checkpoint_store`` driven by a DECLARED cadence rather than an input."""

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], None]) -> None:
        self.upsert(value, ctx)


bench_checkpoint = flezha.tasklet(BenchCheckpoint, name="bench_checkpoint_tasklet")


def _bench_checkpoint() -> dict[str, Any]:
    """The bench's cadence, and its held outputs when the bench asks for them."""
    checkpoint: dict[str, Any] = {"every": _env_int("FLEZHA_BENCH_CHECKPOINT_EVERY", 500) or 500}
    if os.environ.get("FLEZHA_BENCH_CHECKPOINT_OUTPUTS") == "at_checkpoint":
        checkpoint["outputs"] = "at_checkpoint"
        checkpoint["max_delay_ms"] = _env_int("FLEZHA_BENCH_CHECKPOINT_DELAY_MS", 50) or 50
    return {"flow": {"exactly": {"effectively_once": {"ack": {"checkpoint": checkpoint}}}}}


@flezha.relay("bench_checkpoint", run=_bench_checkpoint())
def bench_checkpoint_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").on_full("drop_nothing").into(bench_checkpoint).keyed().output(
        "sink"
    )


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
    c.input("input", codec="text").into(session_timeout).output("kafka")


# -- timer_cap ---------------------------------------------------------------
#
# The core's refusal of a Cell's timers when the item settles (`cell_timers`):
# past the cap of two pending timers, or an event-time timer under a tasklet
# whose clock is not event time. `event` asks for the event-time timer, a
# number `n` for a processing-time one `n` hours ahead; the change follows.


class TimerCap:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], bytes]) -> None:
        if value == "event":
            ctx.timer_at_event_time(1)
        else:
            ctx.timer_after(int(value) * 3_600_000)
        ctx.change({"touched": value})

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"touched": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["touched"].append(change["touched"])


timer_cap = flezha.tasklet(TimerCap, name="timer_cap_tasklet", max_timers_per_cell=2)


@flezha.relay("timer_cap")
def timer_cap_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(timer_cap).output("kafka")


# -- timer_churn -------------------------------------------------------------
#
# The bench factory of `mix bench --shape timer`: each input arms a 1 ms
# processing-time timer and cancels the one the previous input armed; the
# firing emits `fired:<at>`.


class TimerChurn:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], bytes]) -> None:
        at = ctx.timer_after(1)
        last = ctx.state["last"]
        if last != 0 and last != at:
            ctx.cancel_timer("processing_time", last)
        ctx.change({"armed": at})
        ctx.emit(value.encode())

    def on_timer(self, timer: dict[str, Any], ctx: flezha.Ctx[dict[str, Any], bytes]) -> None:
        ctx.emit(f"fired:{int(timer['scheduled_at'])}".encode())

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"last": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["last"] = change["armed"]


timer_churn = flezha.tasklet(TimerChurn, name="timer_churn_tasklet")


@flezha.relay("timer_churn")
def timer_churn_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(timer_churn).output("echo")
