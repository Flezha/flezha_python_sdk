"""The latest-document router :meth:`Channels.route_latest` expands to.

A dynamically keyed procedure cannot receive a broadcast, and fanning an
operative-wide document out over a remembered key set emits keys this member
may no longer own after a rescale. The router delivers on a key's own next
traffic item instead, so every key it emits is one whose traffic just proved
this member owns it, at the price of one item of latency per key.

The expansion is a merge into one single-entity procedure whose
``{document, version, delivered}`` state rides the ordinary change journal:
authoring sugar over existing primitives, with no runtime path of its own.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from . import codec as _codec
from .procedure import procedure

if TYPE_CHECKING:
    from .ctx import Ctx

__all__ = ["ROUTER", "document_input", "traffic_input"]


def document_input(document: Any) -> dict[str, Any]:
    """The merge mapping off the document stream."""
    return {"document": document}


def traffic_input(key: Any) -> dict[str, Any]:
    """The merge mapping off the traffic stream, after the key function ran.

    The traffic value itself never reaches the router: it holds no application
    values beyond the document.
    """
    return {"traffic": str(key)}


class LatestRouter:
    """Retains the latest document and owes each key one delivery per version."""

    def run(self, value: dict[str, Any], ctx: Ctx[dict[str, Any]]) -> None:
        state = ctx.state
        if "document" in value:
            document = value["document"]
            # Version 0 means nothing is retained. The document is opaque, so
            # equality is its class-codec bytes, which a re-poll repeats.
            if state["version"] == 0 or _codec.encode(document) != _codec.encode(state["document"]):
                ctx.change({"retain": document})
            return
        key = value["traffic"]
        version = state["version"]
        if version == 0 or state["delivered"].get(key, 0) >= version:
            return
        # Emit first: a failed emit rides the channel's retry ladder, and
        # recording the delivery before it would make the retry silent.
        ctx.emit((key, state["document"]))
        ctx.change({"delivered": key, "version": version})

    def initial_state(self, entity_key: str | None) -> dict[str, Any]:
        return {"document": None, "version": 0, "delivered": {}}

    def apply_change(self, state: dict[str, Any], change: dict[str, Any]) -> None:
        if "retain" in change:
            state["document"] = change["retain"]
            state["version"] += 1
        else:
            state["delivered"][change["delivered"]] = change["version"]


ROUTER = procedure(name="route_latest")(LatestRouter)
