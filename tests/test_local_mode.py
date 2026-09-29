"""The Python SDK's local mode: a user's own operator, its declared
two-tasklet graph, and no controller anywhere.

The graph and its expected sink output are read from
``test/fixtures/local_mode_word_count.txt``, which the Rust, Java, Node and
Elixir local-mode suites read too — so every core is compared against one
recorded expectation instead of by inspection.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

import pytest

import contract
from flezha import Channels, Ctx, FlezhaError, LocalOperator, state, tasklet, unbounded_operator

FIXTURE = contract.member("test/fixtures/local_mode_word_count.txt")


@tasklet(name="split")
class Split:
    """One line into its words: the hop to `tally` is the runtime's job."""

    def run(self, line: str, ctx: Ctx[None, Any]) -> None:
        for word in line.split():
            ctx.emit(word.lower())


@state
@dataclass
class Count:
    seen: int = 0


@dataclass
class Seen:
    seen: int


@tasklet(name="tally", state=Count, change=Seen, idle_ttl_seconds=300)
class Tally:
    """One Cell per word, minted on first sight of it."""

    def run(self, word: str, ctx: Ctx[Count, Any]) -> None:
        seen = ctx.state.seen + 1
        ctx.change(Seen(seen=seen))
        ctx.emit(f"{word} {seen}")

    def init_state(self, cell_key: str | None) -> Count:
        return Count()

    def apply_change(self, state: Count, change: Seen) -> None:
        state.seen = change.seen


@unbounded_operator("local_word_count_python")
def word_count(c: Channels) -> None:
    c.input("lines", codec="text").into(Split).key_by(lambda word: word).into(Tally).keyed().sink(
        "totals"
    )


def scenario() -> tuple[list[str], Counter[str]]:
    """The scenario every core runs: the lines to feed, and the sink values."""
    lines: list[str] = []
    totals: Counter[str] = Counter()
    for row in FIXTURE.read_text().splitlines():
        if row.startswith("lines: "):
            lines.append(row.removeprefix("lines: "))
        elif row.startswith("totals: "):
            totals[row.removeprefix("totals: ")] += 1
    return lines, totals


def test_a_declared_graph_runs_locally_and_releases_the_recorded_output() -> None:
    lines, expected = scenario()
    released: list[Any] = []
    with LocalOperator.start(word_count) as run:
        run.on_sink("totals", released.append)
        for line in lines:
            run.feed("lines", line)
        run.run_until_idle(20_000)

    counted = Counter(output.payload.decode() for output in released)
    assert counted == expected, (
        "both tasklets ran, the runtime performed the hop, and one Cell per word counted"
    )

    first = released[0]
    assert first.channel == "totals"
    for stamp in ("flz.cell", "flz.seq", "flz.key"):
        assert stamp in first.metadata, f"a keyed sink carries {stamp}: {first.metadata}"


def test_a_channel_the_class_does_not_declare_is_refused_by_name() -> None:
    with LocalOperator.start(word_count) as run, pytest.raises(FlezhaError) as caught:
        run.feed("nope", "1")
    assert caught.value.code == "runtime.channel_undeclared"
    details = caught.value.details
    assert details["channel"] == "nope" and "lines" in details["channels"], (
        f"the refusal names the channel and what is declared: {details}"
    )


@tasklet(name="defaults")
class Defaults:
    """Reads the map an undeclared entry decodes, and emits a string and a map."""

    def run(self, value: Any, ctx: Ctx[None, Any]) -> None:
        ctx.emit(f"a is {value['a']}")
        ctx.emit({"a": value["a"]})


@unbounded_operator("local_default_codecs_python")
def default_codecs(c: Channels) -> None:
    c.input("in").into(Defaults).sink("out")


calls: list[str] = []


@unbounded_operator("local_echoless_python", input="ticks", codec="text")
def echoless(tick: str) -> str:
    calls.append(tick)
    return f"got {tick}"


def test_a_sinkless_short_form_whose_run_returns_a_value_runs_it_once() -> None:
    with LocalOperator.start(echoless) as run:
        run.feed("ticks", "a")
        run.run_until_idle(5_000)
    assert calls == ["a"], "a value with nowhere to go is left alone, never retried"


def test_an_undeclared_entry_reads_json_and_an_undeclared_sink_follows_the_value() -> None:
    released: list[bytes] = []
    with LocalOperator.start(default_codecs) as run:
        run.on_sink("out", lambda output: released.append(bytes(output.payload)))
        run.feed("in", '{"a":1}')
        run.run_until_idle(5_000)
    assert released == [b"a is 1", b'{"a":1}']
