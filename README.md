# flezha

The Flezha Python SDK — a poll host over `flz_core`, the runner kernel the
Rust, Java, Node and Ruby SDKs also drive.

The kernel owns transport, ordering, per-cell exclusivity, durability,
retries, timers, credit, routing and bounded runs. This package brings
the authoring surface, the codec for user types, the threading host and
idiomatic errors — and no kernel code.

```python
import flezha


@flezha.tasklet(name="echo")
class Echo:
    def run(self, value: str, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(value)


@flezha.unbounded_operator("echo")
def echo_operator(c: flezha.Channels) -> None:
    c.input("input", codec="text").into(Echo).sink("out")


flezha.create_runner("default").register_factory(echo_operator).run()
```

- **Preview an output while it is produced.** `ctx.part(value)` sends a
  listener a preview of the tasklet's output while `run` is still working; a
  sink never sees it.
- **Run a graph with no controller.** `LocalOperator.start` takes the same
  operator and runs its declared graph in this process — for trying Flezha out
  and for developing an operator. State is held in memory and nothing is
  durable; every run says so.
- **Python 3.11+**, CPython only. Two wheel tiers from one source: an `abi3`
  compatibility wheel per platform, and per-version free-threaded wheels.
- **The GIL bounds only handler bodies.** Every blocking core call releases it,
  so transport, framing, durability, retries, credit, routing and the
  activation decode all run in parallel in Rust.
- **Typed end to end**, shipping `py.typed`: a wrong edge is a type-checker
  error before it is a registration error.
- **A channel names its payload format.** `codec=` on `input` and `sink` takes
  `"bytes"`, `"text"`, `"json"` or `"msgpack"`. With none, an entry reads JSON and
  a sink sends bytes unchanged, a string as UTF-8 and anything else as JSON, so a
  sink needs no codec for a string, bytes or a JSON value. An Elixir entry
  declared with no decoder hands over the binary instead.
- **Never `pickle`.** Durable state is MessagePack from author-declared
  dataclasses, Pydantic models or attrs classes.
- **One error type.** Anything Flezha refuses raises `flezha.FlezhaError`: a
  program branches on its `code` (such as `topology.duplicate_factory_id` or
  `operator.already_running`) or `class_`, and reads its `message`, `details`
  and `hint`.

Full documentation:
[`docs/sdk/python_sdk.md`](https://github.com/flezha/flezha/blob/main/docs/sdk/python_sdk.md).

Licensed under Apache-2.0.
