"""An emit of the wrong type, and a consumer wired after a sealed channel.

A standard type checker must report exactly these two calls: the context names
the output a tasklet hands on, and a sealed channel has no builder methods.
"""

from __future__ import annotations

import flezha


@flezha.tasklet(name="shout")
class Shout:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(42)


@flezha.unbounded_operator("sealed")
def sealed(c: flezha.Channels) -> None:
    c.input("words").into(Shout).sink("out").sealed().into(Shout)
