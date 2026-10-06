# flezha

The Flezha Python SDK — a poll host over `flz_core`, the runner kernel the
Rust, Java, Node and Ruby SDKs also drive.

**Support tier:**
[tier 2](https://github.com/flezha/flezha/blob/main/docs/publishing/maturity.md#sdk-python),
passes the full conformance run in CI exactly as tier 1 does, with every capability
present, and is permanently supported; it is not yet run by the maintainer in a live
deployment, so a defect specific to it is likely to be found by its users first.

The kernel owns transport, ordering, per-cell exclusivity, durability,
retries, timers, credit, routing and steps runs. This package brings
the authoring surface, the codec for user types, the threading host and
idiomatic errors — and no kernel code.

```python
import flezha


@flezha.tasklet(name="echo")
class Echo:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)


@flezha.relay("echo")
def echo_relay(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(Echo).output("out")


flezha.create_runner("default").register_factory(echo_relay).run()
```

- **One decorator, one settings tree.** `@flezha.relay` declares every
  relay, over a channels body or, as the short form, one tasklet's body.
  Its keywords are the class settings: `run` (`"flow"`, the default, or
  `{"steps": {"each": "per_input", "on_failure": "keep_progress"}}`), `durable`
  (`False`, `"local"`, the default, or `"remote"`, with `sleep_after` in
  milliseconds; by default a flow never sleeps and a steps class sleeps after
  six hours), `max_duration_ms` and `description`. The
  core refuses a setting under a parent that does not carry it, naming both.
- **Preview an output while it is produced.** `ctx.part(value)` sends a
  listener a preview of the tasklet's output while `run` is still working; an
  output never sees it.
- **Answer a caller, or call another relay.** A channel ends in one `output` or
  one `call(...)`. The first output an input causes answers the outside
  caller of that input, and still reaches the output's plugins and listeners.
  `call("pricing", deadline_ms=500)` or
  `call(factory="agent", deadline_ms=60_000, create=False, channel="turn")`
  sends each value, after the chain's transforms and key, to another relay
  (a factory call reaches the instance the chain's `key_by` names);
  `answer_channel=` names the callee's channel whose output answers; one `codec=`
  writes the request and reads the answer, JSON when none is named. The outcome
  arrives later as a `flezha.Response` on the entry channel
  `<channel>.response`, which must feed a tasklet, the calling one included:
  `request`, `ok`, then `value` or `error` (a `FlezhaError`, `call.timeout`
  once the deadline passes), `target` (the callee's instance id) and `created`.

  ```python
  (
      c.input("in", codec="text")
      .into(Account)
      .named("gateway")
      .key_by(lambda line: line.split(":", 1)[0])
      .call(factory="agent", deadline_ms=30_000, codec="text")
      .into(Outcome)  # Outcome.run takes a flezha.Response[str, str]
  )
  ```
- **A relay's API.** Each entry channel is part of its relay's API, which
  `flz relay info`, the HTTP API and AI agents read. `description=` on `input`
  says what the channel is for. A JSON channel's schema is derived from the
  input type of the tasklet it feeds (a `TypedDict`, a dataclass, or any type
  with `model_json_schema()`), and `schema=` declares one instead. Application
  code calls a channel by value with `runner.call`, beside `runner.send`, and
  gets what the relay's outputs answer.

  ```python
  c.input("place", description="Places an order").into(Place).output("placed")

  placed = runner.call("orders-eu", "place", {"pair": "EUR/USD", "amount": 1000})
  ```
- **Run a graph with no controller.** `LocalRelay.start` takes the same
  relay and runs its declared graph in this process — for trying Flezha out
  and for developing a relay. State is held in memory and nothing is
  durable; every run says so.
- **Python 3.11+**, CPython only. Two wheel tiers from one source: an `abi3`
  compatibility wheel per platform, and per-version free-threaded wheels.
- **The GIL bounds only handler bodies.** Every blocking core call releases it,
  so transport, framing, durability, retries, credit, routing and the
  activation decode all run in parallel in Rust.
- **Typed end to end**, shipping `py.typed`: a wrong edge is a type-checker
  error before it is a registration error.
- **A channel names its payload format.** `codec=` on `input` and `output` takes
  `"bytes"`, `"text"`, `"json"` or `"msgpack"`. With none, an entry reads JSON and
  an output sends bytes unchanged, a string as UTF-8 and anything else as JSON, so an
  output needs no codec for a string, bytes or a JSON value. An Elixir entry
  declared with no decoder hands over the binary instead.
- **Never `pickle`.** Durable state is MessagePack from author-declared
  dataclasses, Pydantic models or attrs classes.
- **One error type.** Anything Flezha refuses raises `flezha.FlezhaError`: a
  program branches on its `code` (such as `topology.duplicate_factory_id` or
  `relay.already_running`) or `class_`, and reads its `message`, `details`
  and `hint`.

Full documentation:
[`docs/sdk/python_sdk.md`](https://github.com/flezha/flezha/blob/main/docs/sdk/python_sdk.md).

Licensed under Apache-2.0.
