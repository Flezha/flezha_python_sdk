"""The ``relay_call`` fixtures: relays that call another relay from
their graph and read the outcome as a :class:`flezha.Response`, the mirror of
``sdk/rust/test_app/src/relay_call.rs``.

Every outcome is written as one line, ``<request>=ok:<answer>@<target>/<created>``
or ``<request>=error:<code>@<target>/<created>``.
"""

from __future__ import annotations

import asyncio
from typing import Any

import flezha
from fixtures_steps import LOOP_KNOBS, stateless_step

#: The deadline every fixture call carries, with UTF-8 requests and answers.
CALL: dict[str, Any] = {"deadline_ms": 30_000, "codec": "text"}

#: The prefix an outcome line carries, which no request does.
OUTCOME = "response "

#: The prefix of a turn that may only reach a session already running.
RESUME = "resume:"


def render(response: flezha.Response[str, str]) -> str:
    """One outcome as the line every SDK's fixture writes."""
    outcome = f"ok:{response.value}" if response.error is None else f"error:{response.error.code}"
    created = "true" if response.created else "false"
    return f"{response.request}={outcome}@{response.target}/{created}"


def is_request(line: str) -> bool:
    return not line.startswith(OUTCOME)


def is_turn(line: str) -> bool:
    return is_request(line) and not line.startswith(RESUME)


def is_resume(line: str) -> bool:
    return line.startswith(RESUME)


def is_outcome(line: str) -> bool:
    return line.startswith(OUTCOME)


def session_of(line: str) -> str:
    """The session a turn or an outcome line belongs to: what precedes its ``:``."""
    return line.removeprefix(OUTCOME).split(":", 1)[0]


def account(name: str) -> flezha.Tasklet[Any, str]:
    """A caller's tasklet: a turn goes on as it came, an outcome as its line."""

    def run(value: Any, ctx: flezha.Ctx[None, Any]) -> None:
        ctx.emit(f"{OUTCOME}{render(value)}" if isinstance(value, flezha.Response) else value)

    return stateless_step(name, run)


def outcome_log(name: str) -> flezha.Tasklet[str, str]:
    """An outcome line without its prefix."""
    return stateless_step(name, lambda value, ctx: ctx.emit(value.removeprefix(OUTCOME)))


def _quote(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    if not value.startswith("silent"):
        ctx.emit(f"price:{value}")


@flezha.relay("call_pricing")
def call_pricing_relay(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(stateless_step("pricing_quote", _quote)).output("quotes")


class OrderPriced:
    def run(self, response: flezha.Response[str, str], ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(render(response))


order_priced = flezha.tasklet(OrderPriced, name="order_priced")


@flezha.relay("call_orders")
def call_orders_relay(c: flezha.Channels) -> None:
    (
        c.input("in", codec="text")
        .into(stateless_step("order_ask", lambda value, ctx: ctx.emit(value)))
        .named("order")
        .call("pricing", **CALL, answer_channel="quotes")
        .into(order_priced)
        .output("priced")
    )


async def _agent_turn(value: str, ctx: flezha.Ctx[None, Any]) -> None:
    text = value.split(":", 1)[1] if ":" in value else value
    if text == "boom":
        raise flezha.NonRetryableError("the turn failed")
    if text == "slow":
        await asyncio.sleep(2)
    ctx.emit(f"answer:{value}")


@flezha.relay(
    "call_agent",
    **{**LOOP_KNOBS, "run": {"steps": {"each": "per_input", "on_failure": "keep_progress"}}},
)
def call_agent_steps(c: flezha.Channels) -> None:
    c.input("turn", codec="text").into(stateless_step("agent_turn", _agent_turn)).output()


gateway_resume = stateless_step(
    "gateway_resume", lambda value, ctx: ctx.emit(value.removeprefix(RESUME))
)


@flezha.relay("call_gateway", run={"flow": {"exactly": "effectively_once"}})
def call_gateway_relay(c: flezha.Channels) -> None:
    gateway_account = account("gateway_account")
    gateway = c.input("in", codec="text").into(gateway_account).named("gateway")
    (
        gateway.filter(is_turn)
        .key_by(session_of)
        .call(factory="call_agent", **CALL)
        .into(gateway_account)
    )
    (
        gateway.filter(is_resume)
        .into(gateway_resume)
        .named("resume")
        .key_by(session_of)
        .call(factory="call_agent", create=False, **CALL)
        .into(gateway_account)
    )
    (
        gateway.filter(is_outcome)
        .key_by(session_of)
        .into(outcome_log("gateway_log"))
        .keyed()
        .output("gateway_out")
    )


@flezha.relay("call_tool_session", **LOOP_KNOBS)
def call_tool_session_steps(c: flezha.Channels) -> None:
    tool_turn = account("tool_turn")
    tool = c.input("turn", codec="text").into(tool_turn).named("tool")
    tool.filter(is_request).call("pricing", **CALL).into(tool_turn)
    tool.filter(is_outcome).into(outcome_log("tool_log")).output("tool_out")


#: Every relay this module registers, in the reference's order.
ALL: list[flezha.Relay] = [
    call_pricing_relay,
    call_orders_relay,
    call_agent_steps,
    call_gateway_relay,
    call_tool_session_steps,
]
