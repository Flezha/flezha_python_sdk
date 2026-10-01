"""Helpers every fixture module shares: the key convention, the environment
readers the tunable fixtures use, and the pass-through tasklet several
operators declare identically.
"""

from __future__ import annotations

import os
from typing import Any

import flezha


def _fixture_durable() -> Any:
    """A kept instance that sleeps after `FLEZHA_FIXTURE_SLEEP_AFTER_MS`, so a
    test can watch one sleep; the class default when the variable is unset."""
    sleep_after = _as_int(os.environ.get("FLEZHA_FIXTURE_SLEEP_AFTER_MS", ""))
    return {"local": {"sleep_after": sleep_after}} if sleep_after > 0 else "local"


def prefix(value: str) -> str:
    """The key convention every keyed fixture shares: text before the first ':'."""
    return value.split(":", 1)[0]


def _as_int(value: str) -> int:
    try:
        return int(value)
    except ValueError:
        return 0


FIXTURE_DURABLE = _fixture_durable()


def _env_int(key: str, default: int | None) -> int | None:
    """A positive integer from the environment, or the default.

    The e2e sets three of these — a timer timeout, a dynamic-cell TTL and the
    bench's checkpoint cadence — and each was reading it its own way.
    """
    declared = os.environ.get(key, "")
    return int(declared) if declared.isdigit() and int(declared) > 0 else default


class PassThrough:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)


def passthrough(name: str) -> flezha.Tasklet[str, str]:
    """A tasklet that answers its input, under the reference's node name.

    Several fixtures declare exactly this and differ only in the name the
    topology knows them by, which is what the Ruby app spells as a `PassThrough`
    base with one subclass each.
    """
    return flezha.tasklet(PassThrough, name=name)
