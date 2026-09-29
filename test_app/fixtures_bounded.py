"""Twins of the reference's durable structured bounded fixtures
(``test/apps/rust_runner/src/bounded.rs`` and ``bounded_loop.rs``) under the
same factory ids, tasklet names, channel names and output bytes.

Entry payloads and plugin frames are UTF-8 text on the wire, as in the
reference: every entry names the text codec, and a bare ``sink(name)`` sends a
string as its UTF-8 bytes, as the reference's does, so only a sink that prefixes
what it sends declares an encoder. An inter-tasklet hop between two Python
tasklets carries MessagePack, and the core derives that channel's journal
codec from the channel's kind, so no fixture declares one.

A tasklet node owns exactly one output channel, not one per outgoing edge, so
the whole topology vocabulary is edge reuse: a fan-out is one held ``Edge``
wired twice, and a fan-in is two arms naming one tasklet with no node name.
"""

from __future__ import annotations

import asyncio
import bisect
import itertools
import os
from collections.abc import Callable
from typing import Any

import flezha
from fixture_support import prefix

# -- the release gate --------------------------------------------------------

RELEASE_ENV = "FLEZHA_BOUNDED_E2E_RELEASE"


async def block_until_released() -> None:
    """Hold this activation until a runner boots with the release variable set.

    The harness kills the first runner and launches its replacement with the
    variable present, so the gate is a boot-time decision: never settling on the
    first runner, returning at once on the second.

    A future nobody resolves, rather than a wait with a deadline, because this
    host has two executors and one spelling has to serve both. On the thread
    pool ``dispatch.resolve`` drives it to completion on the worker's own loop,
    which parks that pool worker and no other — the core's driver keeps
    answering control pings and the starvation supervisor grows the pool, so a
    sibling tasklet stays dispatchable. Under ``FLEZHA_PYTHON_EXECUTOR=asyncio``
    the same line suspends the one activation and yields the loop.
    """
    if RELEASE_ENV in os.environ:
        return
    await asyncio.get_running_loop().create_future()


def armed(name: str) -> bool:
    """Read at handler time: each scenario arms it on the runner it launches."""
    return name in os.environ


# -- the three factories -----------------------------------------------------


def insert_sorted(items: list[str], value: str) -> None:
    """Insert into a sorted, de-duplicated list.

    The reference's ``SeenState`` is a ``BTreeSet<String>``, chosen because a
    set keeps snapshot bytes deterministic. MessagePack cannot represent a
    ``set`` and the declared starting state is walked at import, so the twin is
    the list it would decode to — kept sorted for the same determinism and
    idempotent for the same re-run after a resume.
    """
    at = bisect.bisect_left(items, value)
    if at == len(items) or items[at] != value:
        items.insert(at, value)


def durable_step(
    name: str,
    *,
    change: Callable[[str], str | None] | None = None,
    emit: Callable[[str, flezha.Ctx[Any, Any]], None] | None = None,
    compensate: Callable[..., Any] | None = None,
    gate: Callable[[str], bool] | None = None,
) -> flezha.Tasklet[str, str]:
    """The reference's ``durable_step!`` macro: a ``str -> str`` step committing
    one idempotent Change keyed by its own name, so a re-run after a resume
    folds to the state it already had.

    ``emit`` and ``compensate`` are the two axes that macro parameterises;
    ``change`` (which value is recorded, ``None`` for none) and ``gate`` (whether this step waits
    for the release gate BEFORE recording it) are the two the reference varies
    by hand-writing the tasklet instead. Carrying all four is what lets every
    durable tasklet in this module be a single call.

    The handler is a coroutine only when a gate is declared. ``resolve`` answers
    a plain value with no event loop at all, and the bench chains run this
    factory sixteen deep at rate.
    """

    def record(value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        recorded = change(value) if change else name
        if recorded is not None:
            ctx.change({"insert": recorded})
        if emit:
            emit(value, ctx)
        else:
            ctx.emit(value)

    class Step:
        def init_state(self, cell_key: str | None) -> dict[str, Any]:
            return {"items": []}

        def apply_change(self, state: dict[str, Any], recorded: dict[str, Any]) -> None:
            insert_sorted(state["items"], recorded["insert"])

        if gate is None:

            def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
                record(value, ctx)

        else:

            async def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
                # A gated step waits BEFORE its change and its emit, so the kill
                # window always finds it as the uncommitted producer while its
                # sibling arm has committed.
                if gate(value):
                    await block_until_released()
                record(value, ctx)

    if compensate is not None:
        # A crash-resumed compensation reconstructs completed stages with EMPTY
        # outputs, so a hook that records an undo does so unconditionally.
        Step.compensate = staticmethod(compensate)
    return flezha.tasklet(Step, name=name)


def stateless_step(name: str, handle: Callable[..., Any]) -> flezha.Tasklet[str, str]:
    """The reference's ``stateless_step!``: no state, no change, the whole
    behaviour is the handler, called with the value and the context."""

    class Stateless:
        # Answers the handler's coroutine when it is async, which the
        # dispatcher awaits; a sync handler answers None.
        def run(self, value: str, ctx: flezha.Ctx[None, Any]):
            return handle(value, ctx)

    return flezha.tasklet(Stateless, name=name)


def failing_step(name: str) -> flezha.Tasklet[str, str]:
    """A terminal step that always fails, which is how a saga is made to unwind."""

    def always_fails(value: str, ctx: flezha.Ctx[None, Any]) -> None:
        raise RuntimeError(f"{name} always fails")

    return stateless_step(name, always_fails)


def undoing(record: str) -> Callable[[list[Any], flezha.Ctx[Any, Any]], None]:
    """A compensation hook whose whole behaviour is emitting one undo record."""

    def compensate(outputs: list[Any], ctx: flezha.Ctx[Any, Any]) -> None:
        ctx.emit(record)

    return compensate


# -- echo_bounded -----------------------------------------------------------
#
# One tasklet, run once: emits its first message back, so the pass ends and
# the controller deregisters the instance.


def _echo(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    ctx.emit(f"echo:{value}")


echo = stateless_step("echo", _echo)


@flezha.bounded_operator("echo_bounded")
def echo_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(echo).sink("out")


# -- typed_once_bounded -----------------------------------------------------
#
# A `once` bounded operator whose entry decodes JSON: its one pass runs over the
# message it was spawned or sent with, prints what it read, emits it to the
# `placed` sink and completes with it.


def _once_placed(value: dict[str, Any], ctx: flezha.Ctx[None, Any]) -> None:
    text = str(value["text"])
    print(f"once_ran {text}", flush=True)
    ctx.emit(f"placed:{text}")
    ctx.complete(text)


once_placed = stateless_step("once_placed", _once_placed)


@flezha.bounded_operator("typed_once_bounded")
def typed_once_bounded(c: flezha.Channels) -> None:
    c.input("order", codec="json").into(once_placed).sink("placed")


# -- commit_block_bounded / sync_durable_bounded ---------------------------
#
# Task A commits durable state, task B blocks until the resumed runner sets the
# release variable — so a SIGKILL lands with A's state already on the WAL and B
# never finished. `sync_durable_bounded` is the same chain declaring
# SYNCHRONOUS remote durability, which is what lets the kill-the-disk drill
# destroy the state directory and still resume with A committed.

commit_a = durable_step("commit_a")


async def _block(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    await block_until_released()


block_b = stateless_step("block_b", _block)


def commit_block_chain(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(commit_a).named("mid").into(block_b)


commit_block_bounded = flezha.bounded_operator("commit_block_bounded")(commit_block_chain)

sync_durable_bounded = flezha.bounded_operator("sync_durable_bounded", sync_durability=True)(
    commit_block_chain
)


# -- optout_commit_block -----------------------------------------------------
#
# The opt-out twin with a live mid-chain signal: task A's output also reaches a
# plugin sink, so the crash-resume drill can observe the mid-run point without
# durable state. A crash re-runs the chain from the top, so the sink sees A's
# marker once per run and no journal file exists.


@flezha.bounded_operator("optout_commit_block", persistent=False)
def optout_commit_block(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(commit_a).sink(
        "mid", encode=lambda value: f"a-ran:{value}".encode()
    ).into(block_b)


# -- tier_commit_block_bounded ----------------------------------------------
#
# The keyed hybrid twin. Task A commits durable state AND emits
# `committed:<input>` under a stable key on a keyed hybrid channel; task B
# blocks. A SIGKILL lands with A's stage committed and B unfinished, and the
# resume re-runs B alone.

tier_commit_a = durable_step(
    "tier_commit_a",
    emit=lambda value, ctx: ctx.emit(f"committed:{value}", key="job"),
)


@flezha.bounded_operator("tier_commit_block_bounded")
def tier_commit_block_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(tier_commit_a).keyed().sink("mid").into(block_b)


# -- saga_block_bounded -----------------------------------------------------
#
# SA and SB commit durable state and compensate; SC fails, triggering
# reverse-order rollback (SB then SA). SA's compensation BLOCKS, so a SIGKILL
# lands mid-rollback with SB's done-marker already durable — the respawn
# finishes SA and SKIPS the already-done SB.


async def _block_compensation(outputs: list[Any], ctx: flezha.Ctx[Any, Any]) -> None:
    await block_until_released()


saga_a = durable_step("saga_a", compensate=_block_compensation)
saga_b = durable_step("saga_b", compensate=lambda outputs, ctx: None)
saga_c = failing_step("saga_c")


@flezha.bounded_operator("saga_block_bounded")
def saga_block_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(saga_a).named("s1").into(saga_b).named("s2").into(saga_c)


# -- keyed_block_bounded / keyed_saga_bounded -----------------------------
#
# The entry's comma-separated key list is split into one value per key, and a
# `key_by` on that value mints one Cell per key of a durable compensable
# tasklet that declares no keys; each Cell commits one idempotent change. The
# third tasklet blocks until the release variable, or fails, so the keyed stage
# is rolled back once per key.


def _split_keys(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    for key in value.split(","):
        ctx.emit(key)


key_split = stateless_step("key_split", _split_keys)
keyed_commit = durable_step("keyed_commit", compensate=lambda outputs, ctx: None)
keyed_fail = failing_step("keyed_fail")


def keyed_commits(c: flezha.Channels) -> Any:
    return (
        c.input("in", codec="text")
        .into(key_split)
        .named("keys")
        .key_by(lambda key: key)
        .into(keyed_commit)
        .named("committed")
    )


@flezha.bounded_operator("keyed_block_bounded")
def keyed_block_bounded(c: flezha.Channels) -> None:
    keyed_commits(c).into(block_b)


@flezha.bounded_operator("keyed_saga_bounded")
def keyed_saga_bounded(c: flezha.Channels) -> None:
    keyed_commits(c).into(keyed_fail)


# -- outbox_block_bounded ---------------------------------------------------
#
# A bounded output's identity. Task A commits durable state AND emits once,
# keyed `k` with one metadata pair, on a keyed hybrid channel; the output rides
# A's Cell record and reaches the sink once A's stage commits. Task B blocks, so
# a SIGKILL lands with A committed and B unfinished.

outbox_a = durable_step(
    "commit_a",
    emit=lambda value, ctx: ctx.emit(value, key="k", metadata={"trace": "t1"}),
)


@flezha.bounded_operator("outbox_block_bounded")
def outbox_block_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(outbox_a).keyed().sink(
        "mid", encode=lambda value: f"a-out:{value}".encode()
    ).into(block_b)


# -- undo_outbox_bounded ----------------------------------------------------
#
# A compensation's undo output. Task UA commits, emits its forward output on a
# hybrid plugin channel, and compensates by EMITTING an undo record, which rides
# UA's Cell's own record and is released once the `compensated` entry naming it
# is durable. Task C fails immediately, so the chain always rolls back; the hook
# BLOCKS, so on the first runner the SIGKILL lands before the undo was written.
# The respawn re-runs the outstanding hook, whose done marker never landed, and
# the undo is delivered exactly once.


async def _undo_after_release(outputs: list[Any], ctx: flezha.Ctx[Any, Any]) -> None:
    await block_until_released()
    ctx.emit("undone")


undo_outbox_a = durable_step("undo_outbox_a", compensate=_undo_after_release)
undo_outbox_c = failing_step("undo_outbox_c")


@flezha.bounded_operator("undo_outbox_bounded")
def undo_outbox_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(undo_outbox_a).sink(
        "mid", encode=lambda value: f"out:{value}".encode()
    ).into(undo_outbox_c)


# -- failed_stage_bounded ---------------------------------------------------
#
# A failed stage delivers nothing. The entry's two values reach one
# `failed_stage` Cell in order: the first emits on a plugin channel, so the
# Cell's record carries an output; the second fails, so the stage never commits
# and no marker ever names that record.


def _fail_on_b(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    if value == "b":
        raise RuntimeError("failed_stage fails on b")
    ctx.emit(value)


failed_stage = stateless_step("failed_stage", _fail_on_b)


@flezha.bounded_operator("failed_stage_bounded")
def failed_stage_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(key_split).into(failed_stage).sink(
        "mid", encode=lambda value: f"lost:{value}".encode()
    )


# -- branch_bounded ---------------------------------------------------------
#
#     BranchA ─┬─▶ BranchB ─▶ BranchD
#              └─▶ BranchC
#
# The tasklet TREE. Every tasklet commits durable state and compensates by
# emitting an undo record, so both tree behaviours are observable at one sink:
#
#   * crash-resume — BranchD BLOCKS, so the SIGKILL lands with A, B and C
#     committed and only D's subtree outstanding. The respawn re-runs D alone,
#     seeded from B's journaled edge, so the sibling branch's `c:` marker
#     appears exactly ONCE across both runs.
#   * compensation — with FLEZHA_BRANCH_FAIL armed, BranchD fails instead of
#     blocking. A, B and C have all completed, so the rollback unwinds the two
#     branches before the root they share: `undo-c`, `undo-b`, `undo-a`.
#
# The inter-task channels are hybrid, which is what makes a non-leaf
# tasklet's forward and undo output visible at the sink.

BRANCH_FAIL_ENV = "FLEZHA_BRANCH_FAIL"

branch_a = durable_step("branch_a", compensate=undoing("undo-a"))
branch_b = durable_step("branch_b", compensate=undoing("undo-b"))

# The sibling leaf: emits straight to the sink, and its undo does too.
branch_c = durable_step(
    "branch_c",
    emit=lambda value, ctx: ctx.emit(f"c:{value}"),
    compensate=undoing("undo-c"),
)


# The deep leaf under BranchB: blocks (crash-resume), or fails (compensation).
async def _branch_leaf(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    if armed(BRANCH_FAIL_ENV):
        raise RuntimeError("branch_d failed")
    await block_until_released()
    ctx.emit(f"d:{value}")


branch_d = stateless_step("branch_d", _branch_leaf)


@flezha.bounded_operator("branch_bounded")
def branch_bounded(c: flezha.Channels) -> None:
    # One channel, two consumers: THE fan-out. Both branches read exactly what
    # BranchA released on it (held edge, wired twice).
    mid = (
        c.input("in", codec="text")
        .into(branch_a)
        .sink("mid", encode=lambda value: f"a:{value}".encode())
    )
    bmid = mid.into(branch_b).sink("bmid", encode=lambda value: f"b:{value}".encode())
    # Fail fast into the one tasklet that fails: the compensation scenario is
    # about the rollback order, not the ladder that precedes it.
    bmid.retry_max(1).into(branch_d).sink("d_out")
    mid.into(branch_c).sink("c_out")


# -- diamond_bounded --------------------------------------------------------
#
#     DiamA ─┬─▶ DiamB1 ─▶ DiamB2(blocks) ─┬─▶ DiamJoin
#            └─▶ DiamC ────────────────────┘
#
# The fan-in DAG. The join accumulates BOTH arms' outputs and is dispatched
# exactly once, after both commit. DiamB2 blocks one level BELOW its sibling arm
# — siblings commit at one level barrier — so a SIGKILL lands with A, B1 and C
# committed and B2 mid-run: the respawn re-runs B2 alone, and the join runs once
# with C's JOURNALED handoff banked and B2's fresh output appended, so `c:`
# appears exactly once across both runs.

diam_a = durable_step("diam_a")
diam_b1 = durable_step("diam_b1")

diam_b2 = durable_step(
    "diam_b2",
    change=lambda value: "diam_b",
    gate=lambda value: True,
    emit=lambda value, ctx: ctx.emit(f"b:{value}"),
)

diam_c = durable_step("diam_c", emit=lambda value, ctx: ctx.emit(f"c:{value}"))


# The join: one `j:`-tagged record per accumulated contribution.
def _join(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    ctx.emit(f"j:{value}")


diam_join = stateless_step("diam_join", _join)


@flezha.bounded_operator("diamond_bounded")
def diamond_bounded(c: flezha.Channels) -> None:
    # Held edge wired twice = the fan-out; two arms naming diam_join with no
    # node name = the fan-in, since two unnamed mentions are one node.
    fan = (
        c.input("in", codec="text")
        .into(diam_a)
        .sink("dfan", encode=lambda value: f"a:{value}".encode())
    )
    fan.into(diam_b1).named("db1").into(diam_b2).sink("db2").into(diam_join)
    fan.into(diam_c).sink("dc").into(diam_join).sink("j_out")


# -- retry_bounded ----------------------------------------------------------
#
# The per-cell retry ladder. Task RA commits durable state, forwards on a
# hybrid channel, and compensates by emitting an undo record; `retry_flaky`
# fails its first FLEZHA_BOUNDED_RETRY_FAILS attempts (none by default — each
# scenario arms it) and then emits `attempt:<n>:<input>`. One armed failure is
# absorbed by the ladder, so the run completes and the sink sees attempt 2;
# arming more failures than the ladder has attempts exhausts it, RA rolls back
# and the run reports an error.

RETRY_FAILS_ENV = "FLEZHA_BOUNDED_RETRY_FAILS"

# Attempts made in this process. A retry re-runs the tasklet of the same
# instance in place and dispatch is cell-affine, so one counter is the whole
# ladder's view; `next` on a count is atomic, which a `+= 1` on a module global
# under a worker pool is not.
_attempts = itertools.count(1)

retry_a = durable_step("retry_a", compensate=undoing("undone"))


def _armed_failures() -> int:
    declared = os.environ.get(RETRY_FAILS_ENV, "")
    return int(declared) if declared.isdigit() else 0


def _flaky(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    attempt = next(_attempts)
    if attempt <= _armed_failures():
        raise RuntimeError(f"flaky attempt {attempt}")
    ctx.emit(f"attempt:{attempt}:{value}")


retry_flaky = stateless_step("retry_flaky", _flaky)


@flezha.bounded_operator("retry_bounded")
def retry_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(retry_a).sink(
        "mid", encode=lambda value: f"res:{value}".encode()
    ).into(retry_flaky).sink("out")


# -- the durable loop family -------------------------------------------------
#
# chat_loop_bounded is the durable-loop + suspension shape. One iteration per
# inbox item: TURN commits the turn to Cell state, so a resumed iteration is
# visible in the WAL, and REPLY emits the answer to the sink. Between turns the
# loop finds an empty inbox at a committed boundary and PARKS — the instance is
# evicted and rehydrated by the next message. The turn whose payload is the
# crash turn BLOCKS while the release variable is unset, so a SIGKILL lands with
# the earlier iterations durable and that one in flight. Keying the block on the
# payload rather than on an invocation count keeps it deterministic across the
# park/rehydrate cycles, which re-run the driver but not the turns. The turn
# `bye` records nothing, still reaches REPLY, and completes the instance with
# the number of turns TURN holds, so the session ends once that iteration
# commits.
#
# stateless_loop_bounded is the same loop MINUS its stateful first tasklet:
# chat_reply alone holds no per-Cell state, so nothing writes a Cell WAL
# and the instance's task-journal reaches the driver only if it rides the bind
# itself.
#
# chat_rewind_bounded is the loop whose replying tasklet can be undone. The
# undo handle lives in Cell state, because a rewind's compensation is rebuilt
# from the journal and is handed NO retained outputs, so the hook reads the last
# thing it said, retracts it and pops it.

CRASH_TURN = "mid-crash"
BYE_TURN = "bye"


def _turn_emit(value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
    ctx.emit(value)
    if value == BYE_TURN:
        ctx.complete(str(len(ctx.state["items"])))


# Keyed by the turn, so the record count is the number of DISTINCT turns that
# ran — a re-run of a committed iteration would show up.
chat_turn = durable_step(
    "chat_turn",
    change=lambda value: None if value == BYE_TURN else value,
    emit=_turn_emit,
    gate=lambda value: value == CRASH_TURN,
)


def _reply(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    ctx.emit(f"reply:{value}")


chat_reply = stateless_step("chat_reply", _reply)


class ChatReplyUndoable:
    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"said": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        if "said" in change:
            state["said"].append(change["said"])
        else:
            state["said"].pop()

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        ctx.change({"said": value})
        ctx.emit(f"reply:{value}")

    def compensate(self, outputs: list[Any], ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        said = ctx.state["said"]
        if not said:
            return
        # Read before the change: it pops `said` as it is recorded.
        turn = said[-1]
        ctx.change({"retracted": True})
        ctx.emit(f"retract:{turn}")


chat_reply_undoable = flezha.tasklet(ChatReplyUndoable, name="chat_reply_undoable")

# A loop here ends on an external stop, or on `bye` where TURN runs; the duration
# backstop keeps a stray instance from outliving the test run.
LOOP_KNOBS: dict[str, Any] = {"loop_mode": "per_input", "max_duration_ms": 600_000}


def chat_chain(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(chat_turn).named("mid").into(chat_reply).sink("out")


chat_loop_bounded = flezha.bounded_operator("chat_loop_bounded", **LOOP_KNOBS)(chat_chain)


@flezha.bounded_operator("stateless_loop_bounded", **LOOP_KNOBS)
def stateless_loop_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(chat_reply).sink("out")


# keyed_loop_bounded is a durable per-input loop keyed on its entry edge by
# the text before the first `:`, so one Cell counts one symbol's messages and
# emits `<symbol>:<count>`: the shape a partition group of bounded operator members
# takes.
class KeyedLoopCount:
    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"value": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["value"] += change["delta"]

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        symbol = prefix(value)
        ctx.change({"delta": 1})
        ctx.emit(f"{symbol}:{ctx.state['value']}")


keyed_loop_count = flezha.tasklet(KeyedLoopCount, name="keyed_loop_count")


@flezha.bounded_operator("keyed_loop_bounded", **LOOP_KNOBS)
def keyed_loop_bounded(c: flezha.Channels) -> None:
    c.input("trades", codec="text").key_by(prefix).into(keyed_loop_count).sink(
        "keyed_loop_bounded_out"
    )


@flezha.bounded_operator("chat_rewind_bounded", **LOOP_KNOBS)
def chat_rewind_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(chat_turn).named("mid").into(chat_reply_undoable).sink("out")


# failing_turn_loop_bounded: a session whose turn can fail after `note`
# committed. `note` records each message and its undo moves the latest one to
# `undone`; `answer` replies `re:<message>` on the sink `answer`, and fails on
# `boom` always, on `boom-once` while the release env is unset (the first
# runner of a crash drill), and on `boom-until-healed` until a `heal` turn set
# its state. failing_turn_all_or_nothing_bounded is the same under
# all-or-nothing.
class Note:
    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"notes": [], "undone": []}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        if "noted" in change:
            state["notes"].append(change["noted"])
        elif state["notes"]:
            state["undone"].append(state["notes"].pop())

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        ctx.change({"noted": value})
        ctx.emit(value)

    def compensate(self, outputs: list[Any], ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        ctx.change({"undone": True})


note = flezha.tasklet(Note, name="note")


class Answer:
    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"healed": False}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["healed"] = True

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        fails = (
            value == "boom"
            or (value == "boom-once" and RELEASE_ENV not in os.environ)
            or (value == "boom-until-healed" and not ctx.state["healed"])
        )
        if fails:
            raise RuntimeError(f"answer refused {value}")
        if value == "heal":
            ctx.change({"healed": True})
        ctx.emit(f"re:{value}")


answer = flezha.tasklet(Answer, name="answer")


def failing_turn_chain(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(note).named("noted").into(answer).sink("answer")


failing_turn_loop_bounded = flezha.bounded_operator(
    "failing_turn_loop_bounded", rollback_mode="keep_progress", **LOOP_KNOBS
)(failing_turn_chain)
failing_turn_all_or_nothing_bounded = flezha.bounded_operator(
    "failing_turn_all_or_nothing_bounded", rollback_mode="all_or_nothing", **LOOP_KNOBS
)(failing_turn_chain)


# The chat loop with the block moved AFTER the root's commit: TURN commits its
# turn, REPLY blocks on the turn `reply-crash` while the release env is unset,
# so a SIGKILL lands between a root's `task_committed` and the pass's
# `iter_committed`.
REPLY_CRASH_TURN = "reply-crash"


async def _blocking_reply(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    if value == REPLY_CRASH_TURN:
        await block_until_released()
    ctx.emit(f"reply:{value}")


blocking_reply = stateless_step("blocking_reply", _blocking_reply)


@flezha.bounded_operator("reply_block_loop_bounded", **LOOP_KNOBS)
def reply_block_loop_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(chat_turn).named("mid").into(blocking_reply).sink("out")


# -- the bench chains --------------------------------------------------------
#
# What `mix bench.bounded --scenario chain` drives: K durable pass-through
# steps in a line, the payload unchanged, so the sink receives the seed input
# and the bench reads its own seq and send timestamp back. Each chain position
# needs its own tasklet, so the sixteen are generated rather than written out;
# the edge names follow the reference, which spells them from the step TYPE
# rather than from the tasklet name.

BENCH_STEPS = [durable_step(f"bench_s{at}") for at in range(16)]


def bench_chain(factory_id: str, steps: int, persistent: bool) -> flezha.Operator:
    @flezha.bounded_operator(factory_id, persistent=persistent)
    def declare(c: flezha.Channels) -> None:
        edge = c.input("in", codec="text").into(BENCH_STEPS[0])
        for at in range(1, steps):
            edge = edge.named(f"edge_BenchS{at}").into(BENCH_STEPS[at])
        edge.sink("out")

    return declare


bench_chain_1 = bench_chain("bench_chain_1", 1, True)
bench_chain_4 = bench_chain("bench_chain_4", 4, True)
bench_chain_16 = bench_chain("bench_chain_16", 16, True)
bench_chain_optout_4 = bench_chain("bench_chain_optout_4", 4, False)


# -- bench_loop_bounded -----------------------------------------------------
#
# The long-lived loop the `turns` scenario drives: the chat loop's shape with a
# BOUNDED per-turn state, so a million-turn soak snapshots the same few bytes at
# every iteration. With `FLEZHA_BENCH_WAKE_AFTER_MS` set each turn arms a timer
# that far out, whose firing sends the bench's wake marker, `wake:<fired_at>`,
# for the timer-wake measurements.

WAKE_AFTER_ENV = "FLEZHA_BENCH_WAKE_AFTER_MS"


class BenchTurn:
    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"turns": 0, "last": ""}

    def apply_change(self, state: dict[str, Any], seen: dict[str, Any]) -> None:
        state["turns"] += 1
        state["last"] = seen["turn"]

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        ctx.change({"turn": value})
        after = os.environ.get(WAKE_AFTER_ENV, "")
        if after.isdigit():
            ctx.timer_after(int(after))
        ctx.emit(value)

    def on_timer(self, timer: dict[str, Any], ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        ctx.emit(f"wake:{timer['scheduled_at']}")


bench_turn = flezha.tasklet(BenchTurn, name="bench_turn")


# A day, not the chat loop's ten minutes: a soak outlives 600 s.
@flezha.bounded_operator(
    "bench_loop_bounded",
    loop_mode="per_input",
    max_duration_ms=24 * 3_600_000,
)
def bench_loop_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(bench_turn).named("mid").into(chat_reply).sink("out")


# -- timer_loop_bounded -----------------------------------------------------
#
# `in -> tick -> relay -> out`, a per-input loop whose `tick` arms a
# processing-time timer of the delay `after:<ms>` names and emits `armed:<at>`;
# its firing emits `fired:<at>` and opens a pass of its own. `slow:<ms>` holds
# `tick` that long, for a firing due mid-pass (bounded.rs). On a runner booted
# with `FLEZHA_BOUNDED_E2E_HOLD` the firing parks at the gate: `timer` inside
# `on_timer`, before `tick` commits; `relay` in `relay`, after.


def _millis(value: str, prefix: str) -> int:
    rest = value[len(prefix) :]
    return int(rest) if rest.isdigit() else 0


class TimerTick:
    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"items": []}

    def apply_change(self, state: dict[str, Any], recorded: dict[str, Any]) -> None:
        insert_sorted(state["items"], recorded["insert"])

    async def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        if value.startswith("after:"):
            at = ctx.timer_after(_millis(value, "after:"))
            ctx.change({"insert": f"armed:{at}"})
            ctx.emit(f"armed:{at}")
            return
        if value.startswith("slow:"):
            await asyncio.sleep(_millis(value, "slow:") / 1000)
        ctx.emit(value)

    async def on_timer(self, timer: dict[str, Any], ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        await _hold_at("timer")
        ctx.emit(f"fired:{timer['scheduled_at']}")


async def _hold_at(point: str) -> None:
    """Park at ``point`` when this runner was booted to hold there, until the release."""
    if os.environ.get("FLEZHA_BOUNDED_E2E_HOLD") == point:
        await block_until_released()


async def _relay(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    if value.startswith("fired:"):
        await _hold_at("relay")
    ctx.emit(f"relay:{value}")


timer_tick = flezha.tasklet(TimerTick, name="tick")
timer_relay = stateless_step("relay", _relay)


@flezha.bounded_operator("timer_loop_bounded", **LOOP_KNOBS)
def timer_loop_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(timer_tick).named("ticked").into(timer_relay).sink("out")


# -- event_timer_bounded ----------------------------------------------------
#
# An entry-fed tasklet whose clock is the message's leading event time; each
# message arms an event-time timer 100 later and emits `seen:<t>`, and the
# firing emits `fired:<at>` (bounded.rs).


class EventTick:
    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"items": []}

    def apply_change(self, state: dict[str, Any], recorded: dict[str, Any]) -> None:
        insert_sorted(state["items"], recorded["insert"])

    def event_time(self, value: str) -> int:
        leading = value.split(":", 1)[0]
        return int(leading) if leading.isdigit() else 0

    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        at = ctx.timer_at_event_time(ctx.now() + 100)
        ctx.change({"insert": f"armed:{at}"})
        ctx.emit(f"seen:{ctx.now()}")

    def on_timer(self, timer: dict[str, Any], ctx: flezha.Ctx[dict[str, Any], Any]) -> None:
        ctx.emit(f"fired:{timer['scheduled_at']}")


event_tick = flezha.tasklet(EventTick, name="event_tick")


@flezha.bounded_operator("event_timer_bounded", **LOOP_KNOBS)
def event_timer_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(event_tick).sink("out")


# -- static_cell_bounded ----------------------------------------------------
#
# A per-input loop whose one tasklet declares three static cells and routes
# every message to two of them; each cell that runs emits `<cell key>:<message>`,
# so one message yields exactly two outputs (bounded.rs).


class StaticCellStep:
    def run(self, value: str, ctx: flezha.Ctx[None, Any]) -> None:
        ctx.emit(f"{ctx.cell_key}:{value}")

    def cells_for(self, value: str) -> list[str]:
        return ["a", "b"]


static_cell_step = flezha.tasklet(
    StaticCellStep, name="static_cell_step", cell_keys=["a", "b", "c"]
)


@flezha.bounded_operator("static_cell_bounded", **LOOP_KNOBS)
def static_cell_bounded(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(static_cell_step).sink("out")


#: Every bounded operator this app registers, in the reference's order.
ALL: list[flezha.Operator] = [
    echo_bounded,
    typed_once_bounded,
    commit_block_bounded,
    sync_durable_bounded,
    optout_commit_block,
    tier_commit_block_bounded,
    saga_block_bounded,
    keyed_block_bounded,
    keyed_saga_bounded,
    outbox_block_bounded,
    undo_outbox_bounded,
    failed_stage_bounded,
    chat_loop_bounded,
    stateless_loop_bounded,
    keyed_loop_bounded,
    chat_rewind_bounded,
    failing_turn_loop_bounded,
    failing_turn_all_or_nothing_bounded,
    reply_block_loop_bounded,
    retry_bounded,
    branch_bounded,
    diamond_bounded,
    bench_chain_1,
    bench_chain_4,
    bench_chain_16,
    bench_chain_optout_4,
    bench_loop_bounded,
    static_cell_bounded,
    timer_loop_bounded,
    event_timer_bounded,
]
