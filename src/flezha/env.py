"""A knob read from the environment and parsed by the core's one rule.

A knob's name, type and default are declared once in ``Flezha.Config.Keys``;
the core parses the text, so a Python runner answers ``"0"``, ``"on"`` or a
typo exactly as every other runtime does.
"""

from __future__ import annotations

import os

from .native import load_extension


def positive(name: str, fallback: int) -> int:
    """A count from the environment, with ``fallback`` for anything that is not one."""
    parsed = load_extension().knob("positive", os.environ.get(name, ""))
    return fallback if parsed is None else parsed
