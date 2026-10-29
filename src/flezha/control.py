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

__all__ = ["Control", "StartResult", "StartOutcome"]

#: What a start did: started the relay, or found a live instance already
#: holding the id it named and left it untouched.
StartOutcome = Literal["started", "already_running"]


@dataclass(frozen=True, slots=True)
class StartResult:
    """What a start answered: the id the relay runs under, the generated one
    when the start named none, and what the start did."""

    id: str
    outcome: StartOutcome


class Control:
    """The administration surface for the namespace a runner proved."""

    __slots__ = ("_core",)

    def __init__(self, core: Core) -> None:
        self._core = core

    def start_relay(
        self,
        factory_id: str,
        *,
        id: str | None = None,
        init_values: Any = None,
        opts: Any = None,
    ) -> StartResult:
        """Start a relay, on the specification ``flz relay start`` takes.

        Without ``id`` the controller generates one, and the answer carries it.
        A start without an id that is retried after a lost answer starts a second
        relay, and a second stream runs until it is stopped; passing an id
        prevents it, because a start of a live id answers ``already_running``.
        """
        fields = _given(factory_id=factory_id, id=id, init_values=init_values, opts=opts)
        answer = self.call("Relay.start", fields)
        return StartResult(answer["id"], answer["outcome"])

    def stop_relay(self, relay_id: str) -> Any:
        """Stop a relay, or the whole partition group it names."""
        return self.call("Relay.stop", relay_id)

    def enable_plugin(
        self,
        name: str,
        provider: str,
        *,
        opts: dict[str, Any] | None = None,
        single: bool = False,
        relay_id: str | None = None,
        hooks: list[Any] | None = None,
    ) -> Any:
        """Enable the plugin ``name`` in this namespace, of the provider
        ``provider`` (for example ``"relay_websocket"``), on its ``opts``.

        ``single`` runs one copy in the whole cluster. ``relay_id`` with
        ``hooks`` binds that relay in the same call.
        """
        fields = _given(
            provider=provider,
            opts=opts or {},
            single=single or None,
            relay_id=relay_id,
            hooks=hooks,
        )
        return self.call("PluginManager.enable", name, fields)

    def bind_plugin(
        self,
        name: str,
        relay_id: str,
        *,
        hooks: list[Any] | None = None,
        channel: str | None = None,
    ) -> Any:
        """Bind the relay ``relay_id`` to the enabled plugin ``name``."""
        return self.call(
            "PluginManager.bind",
            name,
            _given(relay_id=relay_id, hooks=hooks, channel=channel),
        )

    def unbind_plugin(self, name: str, relay_id: str) -> Any:
        """Unbind the relay ``relay_id`` from the plugin ``name``, which stays enabled."""
        return self.call("PluginManager.unbind", name, {"relay_id": relay_id})

    def disable_plugin(self, name: str) -> Any:
        """Disable the plugin ``name``, stopping it wherever it runs."""
        return self.call("PluginManager.disable", name, {})

    def list_relays(self) -> Any:
        """The relays in this namespace across the cluster: ``rows``, one row each
        (its id, health, runner and node, and a partition group's width and
        members), and ``unreachable``, the controllers that did not answer."""
        return self.call("Relay.list")

    def list_plugins(self) -> Any:
        """The plugins enabled in this namespace, one row each: name, provider,
        single, bound relays and where it runs."""
        return self.call("PluginManager.enabled")

    def cell_state(self, relay_id: str, cell: str | None = None) -> Any:
        """A relay's live state, and the change sequence it was read at.

        ``cell`` is ``<tasklet>[/<cell_key>]``: a tasklet's Cells, or one of
        them; ``None`` reads every Cell of the relay.
        """
        return self.call("Relay.cell_state", relay_id, cell)

    def call(self, verb: str, *args: Any) -> Any:
        """Any published function, by name.

        What succeeds follows from the role the namespace is bound to, never
        from a list kept here. ``args`` are the call's arguments other than the
        namespace, which the controller fills from the connection.
        """
        return json.loads(self._core.admin_call(verb, json.dumps(list(args))))


def _given(**fields: Any) -> dict[str, Any]:
    """The named parameters a call was given, leaving out every one it was not."""
    return {key: value for key, value in fields.items() if value is not None}
