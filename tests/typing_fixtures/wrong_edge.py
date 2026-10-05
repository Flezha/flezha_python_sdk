"""A graph wiring an `Invoice` producer into a `Shipment` consumer.

A standard type checker must report the mismatch here, naming both handlers —
which is what this SDK has in place of the Rust SDK's `tests/ui` compile-fail
pins. The registration-time refusal is the backstop, not the first line.
"""

from __future__ import annotations

from dataclasses import dataclass

import flezha


@dataclass
class Order:
    id: str


@dataclass
class Invoice:
    order_id: str


@dataclass
class Shipment:
    tracking: str


@flezha.tasklet(name="bill")
class Bill:
    def run(self, order: Order, ctx: flezha.Ctx[None, Invoice]) -> None:
        ctx.emit(Invoice(order_id=order.id))


@flezha.tasklet(name="dispatch")
class Dispatch:
    def run(self, shipment: Shipment, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(shipment.tracking)


@flezha.relay("mismatched")
def mismatched(c: flezha.Channels) -> None:
    # `Bill` produces Invoice; `Dispatch` accepts Shipment.
    c.input("orders").into(Bill).named("invoices").into(Dispatch).output()
