"""Administering the namespace this runner's connection proved.

No tasklet has to be running and the application holds no credential of its
own: the controller judges every call as the identity the connection already
established, so nothing an unattended process holds expires. A user grants
a namespace's applications the authority once and revokes it the same way, and
a revocation takes effect on the next call::

    flz role bind runner@team-orders app team-orders

Every method here is one line over a single native entry point, because the
verb travels as data. Adding a verb costs a method and no native work.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal

from .native import Core

__all__ = ["Admin", "StartResult", "StartOutcome"]

#: What a start did: started the operator, or found a live instance already
#: holding the id it named and left it untouched.
StartOutcome = Literal["started", "already_running"]


@dataclass(frozen=True, slots=True)
class StartResult:
    """What a start answered: the id the operator runs under, the generated one
    when the start named none, and what the start did."""

    id: str
    outcome: StartOutcome


class Admin:
    """The administration surface for the namespace a runner proved."""

    __slots__ = ("_core",)

    def __init__(self, core: Core) -> None:
        self._core = core

    def start_operator(
        self,
        factory_id: str,
        *,
        id: str | None = None,
        init_values: Any = None,
        opts: Any = None,
    ) -> StartResult:
        """Start an operator, on the specification ``flz operator start`` takes.

        Without ``id`` the controller generates one, and the answer carries it.
        A start without an id that is retried after a lost answer starts a second
        operator, and a second stream runs until it is stopped; passing an id
        prevents it, because a start of a live id answers ``already_running``.
        """
        fields: dict[str, Any] = {"factory_id": factory_id}
        if id is not None:
            fields["id"] = id
        if init_values is not None:
            fields["init_values"] = init_values
        if opts is not None:
            fields["opts"] = opts
        answer = self.call("Operator.start", fields)
        return StartResult(answer["id"], answer["outcome"])

    def stop_operator(self, operator_id: str) -> Any:
        """Stop an operator, or the whole partition group it names."""
        return self.call("Operator.stop", operator_id)

    def send(self, operator_id: str, message: str, channel: str = "") -> Any:
        """Inject a message into an operator's input stream, blocking for the verdict."""
        return self.call("Operator.send", operator_id, message, channel)

    def enable_plugin(self, plugin_id: str, fields: dict[str, Any]) -> Any:
        """Enable a plugin, built from the fields ``flz plugin enable`` takes."""
        return self.call("PluginManager.enable", plugin_id, fields)

    def disable_plugin(self, plugin_id: str) -> Any:
        """Disable a plugin, or every partitioned instance of it."""
        return self.call("PluginManager.disable", plugin_id)

    def list_operators(self) -> Any:
        """The operator ids running in this namespace."""
        return self.call("Operator.list_operators")

    def list_plugins(self) -> Any:
        """The plugins enabled on the controller node answering this call."""
        return self.call("PluginManager.list")

    def cell_state(self, operator_id: str, cell_key: str) -> Any:
        """One cell's live state, and the change sequence it was read at."""
        return self.call("Operator.cell_state", operator_id, cell_key)

    def call(self, verb: str, *args: Any) -> Any:
        """Any published function, by name.

        What succeeds follows from the role the namespace is bound to, never
        from a list kept here. ``args`` are the call's arguments other than the
        namespace, which the controller fills from the connection.
        """
        return json.loads(self._core.admin_call(verb, json.dumps(list(args))))
