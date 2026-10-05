#!/usr/bin/env python3
"""The Python SDK's e2e test app.

Registers the reference fixture twins and runs until shutdown. The namespace is
``argv[1]`` (default ``default``), matching rust_runner / java_runner /
node_runner / ruby_runner / php_runner / elixir_runner. The executor is an axis
of the run rather than a property of this file: ``FLEZHA_PYTHON_EXECUTOR``
selects the thread pool (the default) or the asyncio loop, and the fixtures are
identical under both.

Two environment variables select a conformance run instead of the fixtures:
``FLEZHA_CONFORMANCE_REPORT`` writes the refusal table's verdicts and exits
without connecting, and ``FLEZHA_REFUSAL_FIXTURE`` registers ONE row of that
table in place of every fixture. Both are ``conformance.py``.

``FLEZHA_LOCAL_SEND_SCRIPT`` is the local-send drill: a JSON array of
``{"relay", "channel", "message", "when"}`` sends this app's own code makes
with ``local=True``, each once the file ``when`` names exists, printing
``local_send <relay> ok`` or ``local_send <relay> err <code>`` per send,
in order.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from pathlib import Path

# The SDK from this checkout, so the app runs against what was just built
# rather than against whatever is installed.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import conformance
import fixtures
import fixtures_call
import fixtures_steps
import flezha


def main() -> None:
    report_at = os.environ.get("FLEZHA_CONFORMANCE_REPORT")
    if report_at:
        conformance.write_report(report_at)
        return

    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging(os.environ.get("FLEZHA_LOG_LEVEL", "info"))

    runner = flezha.create_runner(namespace)
    refusal_fixture = os.environ.get("FLEZHA_REFUSAL_FIXTURE")
    if refusal_fixture:
        # ONE row of the refusal table, in place of every fixture: the drill
        # reads this process's exit status and output, because a runner whose
        # class the core refuses never reaches a controller.
        conformance.register_only(runner, refusal_fixture)
    else:
        for relay in fixtures.ALL:
            runner.register_factory(relay)
        for relay in fixtures_steps.ALL:
            runner.register_factory(relay)
        for relay in fixtures_call.ALL:
            runner.register_factory(relay)

    # The configuration this run is under, before it starts: readiness is the
    # SDK's own startup line, which reports the wheel tier, the executor and the
    # pool sizing.
    print(
        f"[python_runner] namespace={namespace} "
        f"transport={os.environ.get('FLEZHA_TRANSPORT', 'uds')} "
        f"executor={os.environ.get('FLEZHA_PYTHON_EXECUTOR', 'thread')}",
        file=sys.stderr,
        flush=True,
    )

    try:
        runner.start()
        script = os.environ.get("FLEZHA_LOCAL_SEND_SCRIPT")
        if script:
            threading.Thread(target=local_sends, args=(runner, script), daemon=True).start()
        runner.await_termination()
    except flezha.FlezhaError as refusal:
        if not refusal.code.startswith("topology."):
            raise
        # A refused declaration names itself: its codes ride the error as
        # data, and the drill reads them off this process's output.
        print(
            f"[python_runner] declaration refused: "
            f"{', '.join(conformance.codes_of(refusal))}\n{refusal}",
            file=sys.stderr,
            flush=True,
        )
        raise SystemExit(1) from refusal


def local_sends(runner: flezha.Runner, script: str) -> None:
    """Each send of the drill once the test says go, by creating the file
    ``when`` names: the test first proves the relay's sink is live, which a
    send made at bind would race."""
    for send in json.loads(script):
        relay = send["relay"]
        while not os.path.exists(send["when"]):
            time.sleep(0.05)
        try:
            runner.send(relay, send["message"], send["channel"], local=True)
            print(f"local_send {relay} ok", flush=True)
        except flezha.FlezhaError as refusal:
            print(f"local_send {relay} err {refusal.code}", flush=True)


if __name__ == "__main__":
    main()
