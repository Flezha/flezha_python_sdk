# flezha

The Flezha Python SDK — a poll host over `flz_core`, the runner kernel the
Rust, Java, Node, Ruby, PHP, Elixir and Go SDKs also drive.

**Support tier:**
[tier 2](https://github.com/flezha/flezha/blob/main/docs/public/publishing/maturity.md#sdk-python),
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
def echo_relay(c: flezha.ChannelBuilder) -> None:
    c.input("input", codec="text").into(Echo).output("out")


flezha.create_runner("default").register_factory(echo_relay).run()
```

- **One decorator, one settings tree.** `@flezha.relay` declares every
  relay, over a channels body or, as the short form, one tasklet's body.
  Its keywords are the class settings: `run` (`"flow"`, the default, or
  `{"steps": {"each": "per_input", "on_failure": "keep_progress"}}`), `durable`
  (`False`, `"local"`, the default, or `"remote"`, with `sleep_after` in
  milliseconds; by default a flow never sleeps and a steps class sleeps after
  six hours, and with `keep_finished`: a finished instance keeps nothing unless
  it gives hours, or `True` for the controller's window,
  `FLEZHA_STEPS_RETENTION_HOURS`, 72 by default, so `flz relay info` can still
  show its Cells and result), `max_duration_ms` and `description`. The
  core refuses a setting under a parent that does not carry it, naming both.
- **Preview an output while it is produced.** `ctx.part(value)` sends a
  listener a preview of the tasklet's output while `run` is still working; an
  output never sees it.
- **The GIL bounds only handler bodies.** Every blocking core call releases it,
  so transport, framing, durability, retries, credit, routing and the
  activation decode all run in parallel in Rust.
- **Typed end to end**, shipping `py.typed`: a wrong edge is a type-checker
  error before it is a registration error.
- **A channel names its payload format.** `codec=` on `input` and `output` takes
  `"bytes"`, `"text"`, `"json"` or `"msgpack"`. With none, an entry reads JSON and
  an output sends bytes unchanged, a string as UTF-8 and anything else as JSON, so an
  output needs no codec for a string, bytes or a JSON value.
- **Never `pickle`.** Durable state is MessagePack from author-declared
  dataclasses, Pydantic models or attrs classes.
- **One error type.** Anything Flezha refuses raises `flezha.FlezhaError`: a
  program branches on its `code` (such as `topology.duplicate_factory_id` or
  `relay.already_running`) or `class_`, and reads its `message`, `details`
  and `hint`.

## Install

```bash
pip install flezha
```

`flezha` 0.1.0 is not on PyPI yet. Until it is, build a wheel from a checkout as
[`examples/echo`](examples/echo/README.md#building-from-a-checkout) shows.
Optional extras: `flezha[pydantic]`, `flezha[attrs]`, `flezha[uvloop]`; the SDK
imports none of them unconditionally.

## Supported Python versions

Python **3.11 or newer**, CPython only. The native module needs the CPython C
API, so PyPy and GraalPy are out of scope.

Two wheel tiers ship from one source, with one API and one set of semantics:

| Tier | Wheels | What it is for |
| --- | --- | --- |
| Compatibility | one `abi3` wheel per platform, targeting 3.11 | every supported interpreter at or above the floor |
| Performance | per-version free-threaded wheels, from `cp314t` | handler bodies that execute in parallel |

`abi3` has no free-threaded form before CPython 3.15's `abi3t`, which is why the
performance tier is per version. It starts at 3.14t, because PyO3 refuses the
free-threaded build of CPython below 3.14. The runner names its tier in its
startup line.

## The native library

A prebuilt wheel carries the native module, `flezha.flz_core_ffi`, so installing
needs no Rust toolchain. A platform with no prebuilt wheel falls back to the
source distribution, which needs a stable Rust toolchain and fetches the core at
the git tag `native/Cargo.toml` names.

## Local mode, testing and the relay API

- **Local mode**: `LocalRelay.start` runs a relay's declared graph in your own
  process, with no controller. See
  [Local mode](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#local-mode).
- **Testing**: `flezha.testing.TaskletTestHarness` drives one tasklet and checks
  its replay. See
  [Check a tasklet on its own](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/running_and_testing.md#check-a-tasklet-on-its-own).
- **The relay API**: a relay's entry channels, answering a caller, `call(...)`
  to another relay, and `runner.send` and `runner.call` from application code.
  See [Sending and calling](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/sending_and_calling.md).

Full documentation:
[the Python SDK guide](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/python_sdk.md).

Licensed under Apache-2.0.
