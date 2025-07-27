"""
query_orders.py – read‑side examples for the SQLModel order schema
Run:  python query_orders.py
"""

from datetime import datetime, timedelta, UTC
from decimal import Decimal

from sqlmodel import Session, create_engine, select, func
from sqlalchemy.orm import joinedload

import src.db.models as models
from src.db.engine import engine


def list_recent_orders(days: int = 7):
    """Return last `days` orders with eager‑loaded items & marketplace name."""
    since = datetime.now(UTC) - timedelta(days=days)

    stmt = (
        select(models.Order)
        .where(models.Order.created_at >= since)
        .options(
            joinedload(models.Order.items).joinedload(models.OrderItem.product),
            joinedload(models.Order.marketplace),
        )
        .order_by(models.Order.created_at.desc())
    )
    with Session(engine) as session:
        for order in session.exec(stmt).unique():
            print(
                f"\nORDER {order.external_id} ({order.marketplace.name}) "
                f"{order.created_at:%Y-%m-%d %H:%M} – "
                f"€{order.total_gross}"
            )
            for it in order.items:
                print(
                    f"   • {it.product.sku}  ×{it.quantity}  "
                    f"@ {it.price} {it.currency}"
                )


def find_orders_containing_sku(sku: str):
    """All orders that include a given product SKU."""
    stmt = (
        select(models.Order)
        .join(models.OrderItem)
        .join(models.Product)
        .where(models.Product.sku == sku)
        .options(joinedload(models.Order.marketplace))
    )
    with Session(engine) as session:
        orders = session.exec(stmt).all()
        print(f"\nOrders containing {sku}: {len(orders)} found")
        for o in orders:
            print(f"  {o.external_id} ({o.marketplace.name}) @ {o.created_at:%Y-%m-%d}")


def daily_gmv(start: datetime, end: datetime):
    """
    Gross merchandise value per day in the interval [start, end).
    Returns (date, total_gross, orders_count) rows.
    """
    stmt = (
        select(
            func.date_trunc("day", models.Order.created_at).label("day"),
            func.sum(models.Order.total_gross).label("gmv"),
            func.count(models.Order.id).label("orders"),
        )
        .where(models.Order.created_at >= start, models.Order.created_at < end)
        .group_by(func.date_trunc("day", models.Order.created_at))
        .order_by("day")
    )
    with Session(engine) as session:
        rows = session.exec(stmt).all()
        print("\nDaily GMV:")
        for day, gmv, orders in rows:
            print(f"  {day:%Y-%m-%d}:  €{gmv}  ({orders} orders)")


if __name__ == "__main__":
    # 1. recent orders last week
    list_recent_orders(days=30)

    # 2. find all orders that included SKU "ABC123"
    find_orders_containing_sku("ABC123")

    # 3. GMV from 1 Jan 2025 to today
    jan_1 = datetime(2025, 1, 1)
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    daily_gmv(jan_1, today)

