"""The HTTP side of the desk: a flow operator that carries the HTTP plugin,
answers every request, and calls the session a message is for.
"""

from operator import itemgetter

from admit import Admit
from budget import Budget, admitted, sent, used
from request import Request
from watch import Watch

import flezha

DEADLINE_MS = 60_000


# each request and answer on disk before it runs
@flezha.operator("gateway", run={"flow": {"exactly": "effectively_once"}})
def gateway(c: flezha.Channels) -> None:
    user = itemgetter("user")
    http = c.input("http").map(Request.of)
    http.filter(lambda request: not request.sends()).into(Watch).response()  # a stream, or 404
    verdicts = (
        http.filter(Request.sends)
        .map(sent)
        .key_by(user)  # one cell per user
        .into(Budget)
    )
    verdicts.into(Admit).response()  # 202, or 429 over the budget
    (
        verdicts.filter(admitted)
        .map(itemgetter("message"))
        .key_by(user)  # the user's session
        .call(factory="agent", channel="message", deadline_ms=DEADLINE_MS)
        .map(used)
        .key_by(user)
        .into(Budget)  # the tokens the answer used
    )
