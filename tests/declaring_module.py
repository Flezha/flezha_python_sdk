"""A module a runner names, for the explicit-registration tests.

Importing it must open no socket and start no thread — which is what makes
`register_modules` safe to call before `start`.
"""

from __future__ import annotations

import flezha


@flezha.tasklet
def handle(value: str) -> str:
    return value


@flezha.unbounded_operator("from_module")
def from_module(c: flezha.Channels) -> None:
    c.input("in").into(handle).sink()


@flezha.unbounded_operator(id="from_module_short_form", sink="out")
def from_module_short_form(value: str) -> str:
    return value
