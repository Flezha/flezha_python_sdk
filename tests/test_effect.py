"""``ctx.effect`` (``recorded-effects``): a live input keeps an outside call's
result, a replay is served it without running the function, a replay that
reaches outside some other way fails naming the effect, and the refusals."""

from __future__ import annotations

import json
from typing import Any

import pytest

from flezha import Channels, Ctx, FlezhaError, LocalRelay, relay, tasklet
from flezha.testing import EffectReplayMismatch, TaskletTestHarness

#: How often each outside call ran, by name.
CALLS: dict[str, int] = {}


def model(name: str) -> dict[str, Any]:
    CALLS[name] = CALLS.get(name, 0) + 1
    return {"answer": name, "call": CALLS[name]}


@tasklet(name="effect_asks")
class Asks:
    def run(self, question: str, ctx: Ctx[None, Any]) -> None:
        ctx.emit(ctx.effect("model", lambda: model(question)))


@tasklet(name="effect_asks_async")
class AsksAsync:
    async def run(self, question: str, ctx: Ctx[None, Any]) -> None:
        async def call() -> dict[str, Any]:
            return model(question)

        ctx.emit(await ctx.effect("model", call))


class Upper:
    """A codec: the result is kept as upper-case text and read back lower."""

    def encode(self, value: str) -> bytes:
        return value.upper().encode()

    def decode(self, body: bytes) -> str:
        return bytes(body).decode().lower()


@tasklet(name="effect_codec")
class WithCodec:
    def run(self, word: str, ctx: Ctx[None, Any]) -> None:
        ctx.emit(ctx.effect("tool", lambda: word, codec=Upper()))


def test_a_live_input_runs_the_function_and_a_replay_is_served_its_result() -> None:
    harness: TaskletTestHarness[None] = TaskletTestHarness(Asks)
    live = harness.feed("live-q")
    replayed = harness.feed("live-q", replay=True)

    assert CALLS["live-q"] == 1, "the replay did not run the function"
    assert live.outputs == replayed.outputs == [{"answer": "live-q", "call": 1}]


def test_the_async_form_is_awaitable() -> None:
    harness: TaskletTestHarness[None] = TaskletTestHarness(AsksAsync)
    live = harness.feed("async-q")
    replayed = harness.feed("async-q", replay=True)

    assert CALLS["async-q"] == 1
    assert live.outputs == replayed.outputs == [{"answer": "async-q", "call": 1}]


def test_a_codec_encodes_and_decodes_the_result() -> None:
    harness: TaskletTestHarness[None] = TaskletTestHarness(WithCodec)
    assert harness.feed("Hammer").outputs == ["hammer"]


def test_a_replay_that_runs_a_function_fails_naming_the_effect() -> None:
    @tasklet(name="effect_one_more")
    class OneMore:
        def __init__(self) -> None:
            self.calls = 0

        def run(self, _: str, ctx: Ctx[None, Any]) -> None:
            self.calls += 1
            for _ in range(self.calls):
                ctx.effect("model", lambda: "same")

    harness: TaskletTestHarness[None] = TaskletTestHarness(OneMore)
    harness.feed("q")
    with pytest.raises(EffectReplayMismatch, match="effect 'model' ran its function"):
        harness.feed("q", replay=True)


def test_a_call_made_around_the_effect_on_replay_fails_naming_it() -> None:
    @tasklet(name="effect_around")
    class Around:
        def __init__(self) -> None:
            self.live = True

        def run(self, question: str, ctx: Ctx[None, Any]) -> None:
            if self.live:
                self.live = False
                ctx.emit(ctx.effect("model", lambda: model("around")))
            else:
                ctx.emit(model("around"))

    harness: TaskletTestHarness[None] = TaskletTestHarness(Around)
    harness.feed("q")
    with pytest.raises(EffectReplayMismatch, match="effect 'model' was never asked for"):
        harness.feed("q", replay=True)


@pytest.mark.parametrize("name", ["", "m" * 256])
def test_a_bad_name_is_refused_without_a_retry(name: str) -> None:
    @tasklet(name="effect_bad_name")
    class BadName:
        def run(self, _: str, ctx: Ctx[None, Any]) -> None:
            ctx.effect(name, lambda: pytest.fail("a refused call runs nothing"))

    with pytest.raises(FlezhaError) as caught:
        TaskletTestHarness(BadName).feed("q")
    assert caught.value.code == "effect.bad_name"
    assert getattr(caught.value, "non_retryable", False)


def test_an_effect_inside_an_effect_is_refused() -> None:
    @tasklet(name="effect_nested")
    class Nested:
        def run(self, _: str, ctx: Ctx[None, Any]) -> None:
            ctx.effect("outer", lambda: ctx.effect("inner", lambda: 1))

    with pytest.raises(FlezhaError) as caught:
        TaskletTestHarness(Nested).feed("q")
    assert caught.value.code == "effect.nested"


def test_a_failing_function_keeps_nothing_and_its_error_propagates() -> None:
    @tasklet(name="effect_fails")
    class Fails:
        def __init__(self) -> None:
            self.attempts = 0

        def run(self, _: str, ctx: Ctx[None, Any]) -> None:
            self.attempts += 1
            if self.attempts == 1:
                ctx.effect("model", lambda: (_ for _ in ()).throw(TimeoutError("down")))
            ctx.emit(ctx.effect("model", lambda: "second"))

    harness: TaskletTestHarness[None] = TaskletTestHarness(Fails)
    with pytest.raises(TimeoutError, match="down"):
        harness.feed("q")
    assert harness.feed("q").outputs == ["second"], "the failed call kept nothing"


@relay("effect_local_python")
def effect_local(c: Channels) -> None:
    c.input("in", codec="text").into(Asks).output("out", codec="json")


def test_local_mode_runs_the_function_each_time_through_the_core() -> None:
    released: list[Any] = []
    with LocalRelay.start(effect_local) as run:
        run.on_output("out", released.append)
        run.feed("in", "local-q")
        run.feed("in", "local-q")
        run.run_until_idle(20_000)

    assert CALLS["local-q"] == 2, "nothing re-runs a local input, so nothing is kept"
    assert [json.loads(output.payload) for output in released] == [
        {"answer": "local-q", "call": 1},
        {"answer": "local-q", "call": 2},
    ]
