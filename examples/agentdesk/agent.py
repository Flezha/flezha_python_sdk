"""One conversation: a steps operator that runs one turn per message it is
sent, for as long as the session lasts. Every session is an instance of its
own, with its own state.
"""

from typing import TypedDict

from deliver import Deliver
from reply import Reply

import flezha


class Message(TypedDict):  # the session is named after the user
    user: str
    text: str


@flezha.operator(
    "agent",
    # one turn per message; a failed turn fails alone
    run={"steps": {"each": "per_input", "on_failure": "keep_progress"}},
    durable={"local": {"sleep_after": 10_000}},  # an idle session sleeps after 10 s
)
def agent(c: flezha.Channels) -> None:
    (
        c.input("turn", codec="text")  # entry 1: `flz operator call`
        .into(Reply)
        .sink("answer")  # to whoever listens; the chain goes on
        .into(Deliver)
        .response()  # to whoever called
    )
    (
        c.input("message")  # entry 2: the gateway, into the same Reply
        .map(lambda message: message["text"])
        .into(Reply)
    )
