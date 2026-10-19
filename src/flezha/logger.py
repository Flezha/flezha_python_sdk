"""The SDK's diagnostics, through the standard library's ``logging``.

A library never configures the root logger: that decision belongs to the
application. When a runner or a local run starts and neither the ``flezha``
logger nor the root logger has a handler, :func:`install_default` gives the
``flezha`` logger one writing to standard error at ``FLEZHA_LOG_LEVEL``
(``warn`` when unset), the level every other SDK starts at. An application's
own configuration wins; :func:`configure` is the explicit helper.

The same posture the Rust SDK's ``diag`` defaults and the Elixir SDK's
``Logger`` use provide. No OpenTelemetry: no SDK ships spans, and adding them to
one binding is the exact asymmetry the lockstep rule exists to prevent.
"""

from __future__ import annotations

import logging
import os
import sys

__all__ = ["log", "logger", "configure", "install_default", "log_notices", "ROOT_LOGGER_NAME"]

#: Every logger this SDK writes to lives under this name, so an application
#: silences or routes the whole SDK with one call.
ROOT_LOGGER_NAME = "flezha"

_root = logging.getLogger(ROOT_LOGGER_NAME)
# A library must not write to `stderr` on its own initiative when the
# application has configured nothing. `NullHandler` is how the standard library
# says so, and it is removed by nothing but `configure`.
_root.addHandler(logging.NullHandler())


def log() -> logging.Logger:
    """The SDK's root logger."""
    return _root


def logger(name: str) -> logging.Logger:
    """A child logger, for one part of the SDK."""
    return _root.getChild(name)


def configure(level: str | int | None = None, *, stream: object | None = None) -> logging.Logger:
    """Turn the SDK's diagnostics on, at a documented default.

    The default level is ``FLEZHA_LOG_LEVEL`` or ``warn`` -- the one spelling
    every SDK's runner defaults to: a runner that says
    nothing about a wedge is the failure mode diagnostics exist to close, and
    one that narrates every activation is noise. The default destination is
    standard error, so a runner's output stays on the stream the harness reads.
    """
    if level is None:
        level = os.environ.get("FLEZHA_LOG_LEVEL", "warn")
    if isinstance(level, str):
        level = logging.getLevelNamesMapping().get(level.upper(), logging.WARNING)

    handler = logging.StreamHandler(stream or sys.stderr)  # type: ignore[arg-type]
    handler.setFormatter(logging.Formatter("[flezha] %(message)s"))
    for existing in list(_root.handlers):
        if not isinstance(existing, logging.NullHandler):
            _root.removeHandler(existing)
    _root.addHandler(handler)
    _root.setLevel(level)
    return _root


def install_default() -> None:
    """Install :func:`configure`'s handler when the application configured none."""
    configured = [h for h in _root.handlers if not isinstance(h, logging.NullHandler)]
    if not configured and not logging.getLogger().handlers:
        configure()


_core = logger("core")
_LEVELS = {"info": logging.INFO, "warn": logging.WARNING, "error": logging.ERROR}


def log_notices(taken: list[tuple[str, str, str]]) -> None:
    """Write the core's notices, as ``take_notices`` answers them, to the
    ``flezha.core`` logger; the local-mode banner (``always``) at ``ERROR``,
    which no level hides."""
    for level, _target, text in taken:
        _core.log(_LEVELS.get(level, logging.ERROR), "%s", text)
