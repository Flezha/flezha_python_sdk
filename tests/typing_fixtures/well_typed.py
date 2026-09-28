"""A graph whose edge types line up. A type checker must report nothing here."""

from __future__ import annotations

from dataclasses import dataclass

import flezha


@dataclass
class Order:
    id: str


@dataclass
class Invoice:
    order_id: str
    total: int


@flezha.tasklet(name="bill")
class Bill:
    def run(self, order: Order, ctx: flezha.Ctx[None, Invoice]) -> None:
        ctx.emit(Invoice(order_id=order.id, total=1))


@flezha.tasklet(name="render")
class Render:
    def run(self, invoice: Invoice, ctx: flezha.Ctx[None, str]) -> None:
        ctx.emit(f"{invoice.order_id}:{invoice.total}")


@flezha.unbounded_operator("billing")
def billing(c: flezha.Channels) -> None:
    c.input("orders").into(Bill).named("invoices").into(Render).sink()
