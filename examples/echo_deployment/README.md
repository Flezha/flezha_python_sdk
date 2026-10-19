# Echo: the smallest complete deployment

A controller, one runner offering an `echo` relay, and a message that goes
in and comes back. The runner is the standalone Python hello world in
[`examples/echo`](../echo), and this folder
is the deployment around it.

This is the same `echo`
[Up and running](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md)
starts by hand. Running it here first is the fast way to see the pieces move;
[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
is where you learn what each one is.

## With Docker

```sh
docker compose up -d --wait
```

That builds the runner image and waits for the controller to report ready.
Then start a relay from the declaration the runner offers, and send it
something:

```sh
docker compose exec controller /flezha/bin/flz relay start echo --id my_echo
docker compose exec controller /flezha/bin/flz relay send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

The first line is the delivery verdict, which only says the payload was
accepted. The second is the output: the channel name, a tab, and what the
tasklet emitted.

To watch instead of asking, in one terminal:

```sh
docker compose exec controller /flezha/bin/flz relay listen my_echo
```

and send from another. When you are done:

```sh
docker compose down -v
```

**The first build compiles the SDK's native extension from source, so it takes
a few minutes.** The `flezha` package is not on PyPI yet; once it is, the
build stage in `Dockerfile` is deleted and replaced by one `pip install` line,
and the first run becomes immediate.

## Without Docker

Against a controller you are already running, follow the README in
[`examples/echo`](../echo): the same
`flz relay start` and `flz relay send` as above then apply.

**Do not background it with its standard input closed.** The SDK treats end of
file on standard input as a shutdown request, which is how a supervising
harness stops a runner, so a runner started with no stdin exits immediately.
That is also why the compose file sets `stdin_open: true`.

## What the three parts are

| File | Part | What it does |
|---|---|---|
| `echo_tasklet.py` | the tasklet | the processing stage: prefixes each message |
| `echo_relay.py` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `runner.py` | the runner | this process: registers the echo factory and waits |

The tasklet never says where its output goes; the relay decides that.
That split is what lets the graph be rewired without touching the tasklet.
