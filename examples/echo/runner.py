"""Starts a runner: a process that connects to the controller and serves your
operators.

Declare every operator you want served before `start`. One runner can serve
several, and `register_factory` is the same call for the steps kind.
"""

import sys

from echo_operator import echo_operator

import flezha


def main() -> None:
    namespace = sys.argv[1] if len(sys.argv) > 1 else "default"
    flezha.configure_logging("info")

    # the operators this runner serves
    runner = flezha.create_runner(namespace).register_factory(echo_operator)

    # `run()` is `start()` followed by `await_termination()`. Splitting them
    # leaves room for the admin calls below, and still blocks until shutdown.
    runner.start()  # connects to the controller

    # Everything `flz operator` and `flz plugin` do is also a method here.
    # Uncomment to drive this namespace from the application instead of from a
    # terminal:
    #
    # admin = runner.admin()
    # admin.start_operator("echo", id="my_echo")
    # admin.enable_plugin("writer", "operator_file_sink", opts={"path": "echo-out.txt"})
    # admin.bind_plugin("writer", "my_echo", hooks=[["on_output", {}]])
    # admin.send("my_echo", "hello world", "in")
    #
    # docs/sdk/admin.md has the whole surface. An enforced controller refuses all
    # of it until `flz role bind runner@default app default`.

    # Blocks until shutdown: a SIGTERM, or standard input closing.
    runner.await_termination()


if __name__ == "__main__":
    main()
