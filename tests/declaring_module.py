"""A module a runner names, for the explicit-registration tests.

Importing it must open no socket and start no thread — which is what makes
`register_modules` safe to call before `start`.
"""

from __future__ import annotations

import flezha


@flezha.tasklet(name="handle")
class Handle:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)


@flezha.operator("from_module")
def from_module(c: flezha.Channels) -> None:
    c.input("in").into(Handle).sink()


@flezha.operator(id="from_module_short_form", sink="out")
def from_module_short_form(value: str) -> str:
    return value
