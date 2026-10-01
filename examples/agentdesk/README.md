# Agentdesk: the Python project for guides Part 3

The chat sessions the [Part 3 guides](../../../../docs/guides/advanced/advanced_topics.md)
build, as a standalone project. Nothing here depends on the Flezha source tree:
`requirements.txt` names one released version of `flezha`, the way your own
project would. It needs Python 3.11 or later.

Every conversation is a session of its own: an instance of the steps operator `agent`,
which runs one turn per message and keeps the conversation as its state. A turn is
answered by a demo model that lives in this project. It is a stand-in, not a model:
it answers every message with the same sentence, one token at a time, and reports how
many tokens it used. `demo_model.py` is where a real model's client would be called.

In front of the sessions is `gateway`, a flow operator that carries the HTTP
plugin: a POST is a message for a user's session, answered 202 at once, a GET streams
the session's answer as it is written, and each user has a daily token budget.

| File | What is in it |
|---|---|
| `runner.py` | the runner: this process joins the `agents` namespace and registers the `agent` and `gateway` factories |
| `agent.py` | a session, a steps operator: one turn per message, and a failed turn fails alone; and the `Message` the gateway sends it |
| `reply.py` | answers a message with the demo model, sends the answer as it grows, and keeps the conversation |
| `deliver.py` | hands the finished answer to whoever called |
| `demo_model.py` | the demo model, and the `Answer` it gives |
| `gateway.py` | the HTTP side, a flow operator: it carries the HTTP plugin, answers every request, and calls the session a message is for |
| `request.py` | one HTTP request, as the plugin hands it to the gateway, the two routes the gateway knows, and the `Http` answer |
| `watch.py` | answers a request for a session's events with its stream, and any other request with 404 |
| `budget.py` | keeps the tokens each user's answers used today, and admits a message while they are under the day's budget |
| `admit.py` | answers a message 202 when it goes on to the session, and 429 when its user is over the budget |
| `Dockerfile` | the runner's image: the project and its `vendor/` on Python 3.12 |
| `controller/Dockerfile` | the controller's image: Flezha's own, with the plugin packages in `controller/plugins/` |
| `compose.yaml` | one controller and one runner, in Docker Compose, with the gateway's HTTP port published on this machine only |
| `cluster.yaml` | a second controller with its runner, and a PostgreSQL database both controllers copy their state to; read on top of `compose.yaml` |
| `k8s/agentdesk.yaml` | the same images on Kubernetes: one pod with the controller and the runner, a volume, and a service |

What a tasklet remembers is a `@flezha.state` dataclass, and the values the tasklets
take and hand on are dicts, typed as `TypedDict`s, as in the tradedesk; only the
gateway's input is a `Request`, which the entry channel decodes the plugin's envelope
into.

## Run it

You need Python 3.11 or later with pip, and Docker with Docker Compose.

```sh
python3 -m pip install --target vendor -r requirements.txt
docker compose up -d --build --wait
alias flz='docker compose exec controller bin/flz'
```

The runner's image copies `vendor/` as pip left it. A Python runner stops when its
standard input closes, and a container's is closed, so the image hands the runner a
pipe that stays open. The alias runs `flz` inside the controller's container. The
runner joins `agents` on its own; the controller lists it:

```sh
flz runner list --namespace agents
```

## Talk to a session

Start a session, then send it a turn and wait for its answer:

```sh
flz operator start agent --id anna --namespace agents
flz operator call anna --namespace agents --text 'Hello'
```

```
{"text":"Message 1: you said \"Hello\". I am a demo model.","tokens":10}
```

## Talk to it over HTTP

With the HTTP plugin's package in `controller/plugins/` (the guide's
[`custom_plugins.md`](../../../../docs/guides/advanced/custom_plugins.md) builds it), start
the gateway, enable the plugin on it, and send a message from this machine:

```sh
flz operator start gateway --id gateway --namespace agents
flz plugin enable web http_server --operator_id=gateway --namespace agents --hook=on_terminate --Xport=8080 --Xbind=0.0.0.0 --Xchannel=http
curl -s -d 'Hello from the web' localhost:8080/sessions/anna/messages
curl -sN localhost:8080/sessions/anna/events
```

The example runs with authorization off, and the plugin authenticates nobody: it is a
demonstration, not a setup to expose.

`docker compose down -v` stops both containers and removes the sessions with their volumes.

## Run it as a cluster, or on Kubernetes

With `RELEASE_COOKIE` and `DURABILITY_PASSWORD` set in the shell,
`COMPOSE_FILE=compose.yaml:cluster.yaml docker compose up -d --build --wait` adds a
second controller; the guide's [`clustering.md`](../../../../docs/guides/advanced/clustering.md)
walks it. [`kubernetes_deployment.md`](../../../../docs/guides/advanced/kubernetes_deployment.md)
builds the images and deploys `k8s/agentdesk.yaml` on k3s.

## Building from a checkout

The package is not on PyPI yet, and the controller's image `flezha/flezha:0.1.0` has to
be built. From the SDK's folder, `../..`, build a wheel with maturin, pointed at your
checkout's core, since the core has no release tag yet either:

```sh
pip install "maturin>=1.9,<2.0"
maturin build --release --out dist --features python-extension,python-abi3 --offline \
  --config "patch.\"https://github.com/flezha/flezha\".flz_core_ffi.path=\"$PWD/../../flz_core_ffi\""
```

It needs a Rust toolchain. Build the controller's image from the repository root:

```sh
docker build -f docker/prod/Dockerfile.controller -t flezha/flezha:0.1.0 .
```

Then, in this folder, install the wheel into `vendor/` instead of the
`requirements.txt` line:

```sh
python3 -m pip install --target vendor ../../dist/flezha-0.1.0-*.whl
```

Once the package is published, the step goes away.

## Where to go next

The tradedesk in [`../tradedesk`](../tradedesk/README.md) is the Part 2 project, and
[`docs/sdk/calls.md`](../../../../docs/sdk/calls.md) is the contract `flz operator call`
follows.
