"""An emit of the wrong type.

A standard type checker must report exactly this call: the context names the
output a tasklet hands on.
"""

from __future__ import annotations

import flezha


@flezha.tasklet(name="shout")
class Shout:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(42)
