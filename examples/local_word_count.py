"""The two-tasklet word count, run in this process with no controller.

    .venv/bin/python examples/local_word_count.py

from the package root, once `.venv` holds the package: `scripts/ci.sh` makes it
in this repository, `scripts/build-python-sdk.sh` in the core's.

`Split` and `Tally` are ordinary tasklets with nothing local-mode about
them: the same declaration deploys against a controller unchanged. What local
mode supplies is the sink — the values `Tally` releases arrive here instead of
on a socket.

State is held in memory and nothing is durable; the run says so on stderr.
"""

from __future__ import annotations

from dataclasses import dataclass

from flezha import Channels, Ctx, LocalRelay, relay, state, tasklet


@tasklet(name="split")
class Split:
    def run(self, line: str, ctx: Ctx[None, str]) -> None:
        for word in line.split():
            ctx.emit(word.lower())


@state
@dataclass
class Count:
    seen: int = 0


@dataclass
class Seen:
    seen: int


@tasklet(name="tally", idle_ttl_seconds=300)
class Tally:
    def run(self, word: str, ctx: Ctx[Count, str]) -> None:
        seen = ctx.state.seen + 1
        ctx.change(Seen(seen=seen))
        ctx.emit(f"{word} {seen}")

    def apply_change(self, state: Count, change: Seen) -> None:
        state.seen = change.seen


@relay("local_word_count_example")
def word_count(c: Channels) -> None:
    # One Cell per word: the hop from `Split` to `Tally` is the runtime's,
    # not this file's.
    c.input("lines", codec="text").into(Split).key_by(lambda word: word).into(Tally).keyed().sink(
        "totals"
    )


if __name__ == "__main__":
    with LocalRelay.start(word_count) as run:
        run.on_sink("totals", lambda output: print(output.payload.decode()))
        run.feed("lines", "the quick brown fox")
        run.feed("lines", "the lazy dog")
        run.run_until_idle(10_000)
