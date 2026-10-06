"""The record the core is handed, and the verdict it answers.

Every rule a declared topology can be refused under is one function in
``flz_core::host::register::check``, and :func:`validate` is the call that
submits to it: this SDK records what the author declared, hands it over, and
raises the answer as :class:`~flezha.errors.FlezhaError`. Nothing here restates a rule the core
holds — a refusal added there reaches Python with no edit to this file.

What a type checker *can* see — a wrong value type on a wired edge — is caught
earlier still, by the generic :class:`~flezha.builder.Edge`, and what one
tasklet's own declaration can be refused for is caught earlier still by
:class:`~flezha.tasklet.Tasklet` when the decorator builds it.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from . import codec as _codec
from .builder import ChannelSpec, ClassSpec, TaskNode
from .tasklet import input_type, output_type
from .version import __version__

if TYPE_CHECKING:
    from .dispatch import Registry
    from .native import Core

__all__ = ["declare", "validate", "to_host_class_decl", "SDK_LANG_PYTHON", "FORMAT_ID_MSGPACK"]

# The Python SDK's byte in the core's `sdk_lang` allocation table.
SDK_LANG_PYTHON = 7

# MessagePack, shared with every SDK whose codec is MessagePack.
FORMAT_ID_MSGPACK = 5


def declare(spec: ClassSpec, core: Core, registry: Registry) -> None:
    """Judge, register and declare one class, in the one order that works.

    :func:`validate` writes the derived channel names and consumer edges back
    into the spec, so a caller that skipped it would register a spec whose
    edges route nowhere. Both the runner and local mode reach the core through
    here, so there is one such order rather than two that agree.
    """
    validate(spec, core)
    registry.add(spec)
    core.declare_class(json.dumps(to_host_class_decl(spec)).encode())


def validate(spec: ClassSpec, core: Core) -> dict[str, Any]:
    """Submit the record and answer what the core derived from it.

    On a refusal this raises the core's :class:`~flezha.errors.FlezhaError`:
    its code is the first issue's, and ``details["issues"]`` lists every issue
    as ``{code, message, details}``. On success the derived channel names are
    written back, so the name this host addresses a channel by is the one the
    core registered.
    """
    answer = core.check_class(json.dumps(to_host_class_decl(spec)).encode())
    derived: dict[str, Any] = json.loads(answer)
    for channel in derived["channels"]:
        declared = spec.channels[channel["index"]]
        declared.name = channel["name"]
        # The runtime edges ARE the core's answer: it says which chains
        # produced a consumer and in what order, and the host attaches the
        # closures that chain declared.
        declared.consumers = [declared.chain_edges[edge["chain"]] for edge in channel["consumers"]]
    return derived


# ---------------------------------------------------------------------------
# The declaration the core deserializes
# ---------------------------------------------------------------------------


def _compact(fields: dict[str, Any]) -> dict[str, Any]:
    """Drop the keys whose value is None, so the JSON omits them."""
    return {key: value for key, value in fields.items() if value is not None}


def _value_type(spec: ClassSpec, channel: ChannelSpec) -> Any:
    """The type a channel's API values have, where the declaration shows it: an
    entry's is the input of the tasklet a chain wires it into with no transform,
    an output's the output of the tasklet producing it."""
    if channel.kind == "entry":
        for chain in channel.chains:
            links = [link for link in chain if link["link"] != "key"]
            if links and links[0]["link"] == "into":
                return input_type(spec.tasklets[links[0]["tasklet"]].task)
        return None
    if channel.sink is not None and channel.call_end is None and channel.producer is not None:
        return output_type(spec.tasklets[channel.producer].task)
    return None


def _schema(spec: ClassSpec, channel: ChannelSpec) -> Any:
    if channel.schema is not None:
        return channel.schema
    if channel.codec != "json":
        return None
    return _codec.json_schema(_value_type(spec, channel))


def _channel_json(spec: ClassSpec, channel: ChannelSpec) -> dict[str, Any]:
    return _compact(
        {
            "name": channel.name,
            "kind": channel.kind,
            "producer": channel.producer,
            "chains": channel.chains,
            "retry_max": channel.retry_max,
            "retry_backoff_ms": channel.retry_backoff_ms,
            "overflow": channel.overflow,
            "keyed": channel.keyed,
            "codec": channel.codec,
            "schema": _schema(spec, channel),
            "description": channel.description or None,
        }
    )


def _keyed_consumers(spec: ClassSpec) -> set[int]:
    """The nodes some chain reaches through a `key` link: the cell mode is the
    wiring's, so one tasklet can be keyed in one relay and single in another."""
    keyed: set[int] = set()
    for channel in spec.channels:
        for chain in channel.chains:
            key = False
            for link in chain:
                if link["link"] == "key":
                    key = True
                elif key and link["link"] in ("into", "merge"):
                    keyed.add(link["tasklet"])
    return keyed


def _cell_mode(node: TaskNode, keyed: set[int]) -> str:
    if node.task.cell_keys is not None:
        return "static"
    return "dynamic" if node.index in keyed else "single"


def _tasklet_json(spec: ClassSpec, node: TaskNode, keyed: set[int]) -> dict[str, Any]:
    task = node.task
    ttl = task.idle_ttl_seconds
    return _compact(
        {
            "name": node.name,
            "cell_mode": _cell_mode(node, keyed),
            "cell_keys": task.cell_keys or [],
            "idle_ttl_ms": None if ttl is None else ttl * 1000,
            "max_cells": task.max_cells,
            "cells_for_overridden": task.cells_for is not None,
            "compensate_overridden": task.compensate is not None,
            "clock": task.clock,
            # `is None`, not a falsy test: a declared 0 is what the core
            # refuses as `MailboxCapacityZero`, and rewriting it to the default
            # would hide the very declaration the author made.
            "mailbox_capacity": 1024 if task.mailbox_capacity is None else task.mailbox_capacity,
            # A tasklet that folds no change has the empty Change type, so a
            # logical clock over it would never advance; a class that journals
            # nothing has it for every tasklet.
            "change_is_nochange": not spec.keeps_work or task.apply_change is None,
            "schema_version": task.schema_version or 1,
            "min_readable_schema_version": task.min_readable_schema_version,
        }
    )


def to_host_class_decl(spec: ClassSpec) -> dict[str, Any]:
    """The declaration the core deserializes.

    The settings tree goes as the author wrote it: the core judges it and
    refuses a misplaced setting by name. Every key is pinned by ``test/fixtures/host_class_decl_keys.txt``, which this package's
    own test reads: a key the core learns that Python cannot spell fails that
    test.
    """
    keyed = _keyed_consumers(spec)
    return _compact(
        {
            "factory_id": spec.factory_id,
            "description": spec.description,
            "max_duration_ms": spec.max_duration_ms,
            "run": spec.run,
            "durable": spec.durable,
            "sdk_lang": SDK_LANG_PYTHON,
            "sdk_version": __version__,
            "change_format_id": FORMAT_ID_MSGPACK,
            "compact_after_bytes": spec.compact_after_bytes,
            "channels": [_channel_json(spec, channel) for channel in spec.channels],
            "tasklets": [_tasklet_json(spec, node, keyed) for node in spec.tasklets],
        }
    )
