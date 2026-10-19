"""A graph's call to another relay, and the outcome that comes back.

A ``call(...)`` end sends each value its chain yields to another relay; the
outcome arrives later as a :class:`Response` on the caller's Response channel,
whose payload is the JSON envelope the core reads for :func:`response_decoder`.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Generic, TypeVar

from .errors import FlezhaError

__all__ = ["Response", "call_link", "response_decoder"]

Req = TypeVar("Req")
Ans = TypeVar("Ans")


@dataclass(frozen=True, slots=True)
class Response(Generic[Req, Ans]):
    """The outcome of one call.

    ``request`` is the request as the call end wrote it, decoded back. Exactly
    one of ``value`` (the callee's answer) and ``error`` (what ended the call,
    ``call.timeout`` among them) is set; ``ok`` tells which. ``target`` is the
    callee's instance id and ``created`` whether this call started it.
    """

    request: Req
    target: str
    created: bool
    value: Ans | None = None
    error: FlezhaError | None = None

    @property
    def ok(self) -> bool:
        """Whether the callee answered; ``value`` then holds the answer."""
        return self.error is None


def call_link(
    target: str | None,
    factory: str | None,
    deadline_ms: int,
    create: bool,
    channel: str | None,
    answer_channel: str | None,
    response: str,
) -> dict[str, Any]:
    """The ``call`` link the core reads, its Response channel named ``response``."""
    link: dict[str, Any] = {"link": "call"}
    if target is not None:
        link["target"] = target
    if factory is not None:
        link["factory"] = factory
    link.update(deadline_ms=deadline_ms, create=create)
    if channel is not None:
        link["channel"] = channel
    if answer_channel is not None:
        link["answer_channel"] = answer_channel
    link["response"] = response
    return link


def response_decoder(decode: Callable[[Any], Any]) -> Callable[[Any], Response[Any, Any]]:
    """The Response channel's decoder: the core reads the envelope, then its
    request and its answer each go through ``decode``, the call's codec as
    ``input`` reads it. A payload that is no envelope raises
    ``call.malformed_response``."""
    from .native import load_extension

    module = load_extension()

    def decode_response(body: Any) -> Response[Any, Any]:
        try:
            target, created, request, value, error = module.response_decode(bytes(body))
        except module.NativeError as refused:
            raise FlezhaError.from_document(str(refused)) from refused
        sent = decode(request)
        if error is not None:
            return Response(sent, target, created, error=FlezhaError.from_document(error))
        return Response(sent, target, created, value=decode(value))

    return decode_response
