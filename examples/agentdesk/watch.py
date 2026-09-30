"""Answers a request for a session's events with its stream, and any other
request with 404.
"""

from request import Http, Request

import flezha


@flezha.tasklet(name="watch")
class Watch:
    def run(self, request: Request, ctx: flezha.Ctx[None, Http]) -> None:
        ctx.emit(
            Http(stream=request.user())
            if request.listens()
            else Http(status=404, body="no such route")
        )
