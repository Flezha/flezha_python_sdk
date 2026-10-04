"""Hands the finished answer to the caller: a channel has one end, and `answer`
is a sink.
"""

from demo_model import Answer

import flezha


@flezha.tasklet(name="deliver")
class Deliver:
    def run(self, answer: Answer, ctx: flezha.Ctx[None, Answer]) -> None:
        ctx.emit(answer)  # the whole answer, to whoever called

    # The undo, when a turn is taken back: an answer already given has nothing to take back.
    def compensate(self, outputs: list[Answer], ctx: flezha.Ctx[None, Answer]) -> None:
        pass
