"""One conversation: a bounded operator that runs one turn per message it is
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


@flezha.bounded_operator(
    "agent",
    loop_mode="per_input",  # one turn per message
    rollback_mode="keep_progress",  # a failed turn fails alone
)
def agent(c: flezha.Channels) -> None:
    (
        c.input("turn", codec="text")
        .into(Reply)
        .sink("answer")  # to whoever listens
        .into(Deliver)
        .response()  # to whoever called
    )
    (
        c.input("message")  # from the gateway
        .map(lambda message: message["text"])
        .into(Reply)
    )
