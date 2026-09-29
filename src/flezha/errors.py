"""Everything this SDK raises, so a host can catch one thing."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

__all__ = [
    "FlezhaError",
    "NonRetryableError",
    "DeterminismWarning",
]


class FlezhaError(Exception):
    """Anything Flezha refuses: the controller, the core, or this SDK itself.

    ``code`` (``family.snake``) and ``class_`` (``invalid``, ``not_found``,
    ``denied``, ...) are what a program branches on; ``message`` is one
    sentence for a reader, ``details`` names what the message is about, and
    ``hint`` is the next command to run, when there is one. The text of
    ``str(error)`` is the message followed by the code in parentheses.

    A refusal this SDK makes is built from its code and details, and its
    message is the catalogue's, rendered by the core::

        raise FlezhaError("topology.duplicate_factory_id", {"factory_id": "echo"})
    """

    code: str
    class_: str
    message: str
    details: dict[str, Any]
    hint: str | None

    def __init__(self, code: str, details: Mapping[str, Any] | None = None) -> None:
        from .native import load_extension

        document = load_extension().error_document(code, json.dumps(dict(details or {})))
        self._fill(json.loads(document)["error"])

    @classmethod
    def from_document(cls, document: str) -> FlezhaError:
        """The error a JSON error document ``{"error": {...}}`` carries."""
        error = cls.__new__(cls)
        error._fill(json.loads(document)["error"])
        return error

    def _fill(self, fields: Mapping[str, Any]) -> None:
        self.code = str(fields["code"])
        self.class_ = str(fields["class"])
        self.message = str(fields["message"])
        self.details = dict(fields.get("details") or {})
        hint = fields.get("hint")
        self.hint = None if hint is None else str(hint)
        Exception.__init__(self, self.message)

    def __str__(self) -> str:
        return f"{self.message} ({self.code})"


class NonRetryableError(Exception):
    """A handler error the retry ladder must not retry.

    Raise this — or set ``non_retryable = True`` on any exception — to park an
    item immediately instead of consuming its remaining budget. An author's own
    steering type, not a refusal, so it is no :class:`FlezhaError`.
    """

    non_retryable = True


class DeterminismWarning(UserWarning):
    """An ambient clock or identity read inside an invocation.

    A warning and not an error: the call is legal Python and the invocation
    completes, but its value differs between the live run and the replay, which
    is a defect nothing else reports.
    """
