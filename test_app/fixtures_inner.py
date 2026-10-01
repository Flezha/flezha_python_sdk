"""The ``inner_operator`` fixtures: one stepped session per key inside a flow
gateway, the mirror of ``sdk/rust/test_app/src/inner.rs``.

``key:text`` inputs; each key's session counts its turns in its own state and
answers ``key#turns:text`` on the gateway's ``replies`` sink. The session
sleeps after ``FLEZHA_FIXTURE_SLEEP_AFTER_MS`` and wakes on its key's next
message with its count intact, while the gateway and every other key go on.
Its second stage fails on the text ``fail`` (the turn is undone) and holds
``hold`` until the release env (a crash lands between the two stages); the
gateway's reply holds ``stall`` the same way (a crash lands after the session
answered).

``loop_gateway`` embeds the chained ``countdown``: ``label:n`` counts down one
per pass, and only the converged ``label:0`` reaches the gateway's ``replies``.
"""

from __future__ import annotations

from typing import Any

import flezha
from fixture_support import FIXTURE_DURABLE, prefix
from fixtures_steps import block_until_released, stateless_step


class SessionTurn:
    def run(self, value: str, ctx: flezha.Ctx[dict[str, Any], str]) -> None:
        key, _, text = value.partition(":")
        ctx.change({"delta": 1})
        ctx.emit(f"{key}#{ctx.state['value']}:{text}")

    def init_state(self, cell_key: str | None) -> dict[str, Any]:
        return {"value": 0}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        state["value"] += change["delta"]

    def compensate(self, outputs: list[Any], ctx: flezha.Ctx[dict[str, Any], str]) -> None:
        ctx.change({"delta": -1})


session_turn = flezha.tasklet(SessionTurn, name="session_turn")


async def _check(value: str, ctx: flezha.Ctx[None, str]) -> None:
    text = value.partition(":")[2]
    if text == "fail":
        raise RuntimeError("session_check refuses fail")
    if text == "hold":
        await block_until_released()
    ctx.emit(value)


session_check = stateless_step("session_check", _check)


@flezha.operator("session", run={"steps": {"each": "per_input"}}, durable=FIXTURE_DURABLE)
def session_steps(c: flezha.Channels) -> None:
    # A host tasklet emits only on a channel it declares: this one is the exit.
    c.input("turns", codec="text").into(session_turn).into(session_check).named("answer")


async def _reply(value: str, ctx: flezha.Ctx[None, str]) -> None:
    if value.partition(":")[2] == "stall":
        await block_until_released()
    ctx.emit(value)


GatewayReply = stateless_step("gateway_reply", _reply)


# It journals its input (a kept instance that may sleep), so an answer a session
# passed it is replayed after a crash.
@flezha.operator("session_gateway", durable={"local": {"sleep_after": 3_600_000}})
def gateway_operator(c: flezha.Channels) -> None:
    c.input("requests", codec="text").key_by(prefix).into(session_steps).into(GatewayReply).sink(
        "replies", codec="text"
    )


def _count_down(value: str, ctx: flezha.Ctx[None, str]) -> None:
    label, _, n = value.partition(":")
    if int(n) > 0:
        ctx.emit(f"{label}:{int(n) - 1}")


@flezha.operator("countdown", run={"steps": {"each": "chained"}})
def countdown_steps(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(stateless_step("countdown_step", _count_down)).named("carry")


@flezha.operator("loop_gateway")
def loop_gateway_operator(c: flezha.Channels) -> None:
    c.input("requests", codec="text").into(countdown_steps).into(GatewayReply).sink(
        "replies", codec="text"
    )


#: Every operator this module registers: the embedded classes are not ones.
ALL: list[flezha.Operator] = [gateway_operator, loop_gateway_operator]
