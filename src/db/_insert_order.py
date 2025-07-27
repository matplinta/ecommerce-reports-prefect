from datetime import datetime
from decimal import Decimal

from sqlmodel import Session, create_engine, select

from src.db.models import Marketplace, Product, Order, OrderItem   # your earlier file

DB_URL = "postgresql+psycopg2://dev:secret@localhost:5432/shop"
engine = create_engine(DB_URL, echo=False)

def get_or_create(session, model, defaults=None, **kwargs):
    stmt = select(model).filter_by(**kwargs)
    instance = session.exec(stmt).first()
    if instance:
        return instance
    params = {**kwargs, **(defaults or {})}
    instance = model(**params)
    session.add(instance)
    session.commit()
    session.refresh(instance)
    return instance

def main():
    with Session(engine) as session:
        # 1. marketplace (idempotent)
        amazon = get_or_create(session, Marketplace, name="Amazon")

        # 2. product (idempotent)
        widget = get_or_create(
            session,
            Product,
            sku="ABC123",
            defaults={"name": "Red Widget"},
        )

        # 3. assemble order + item
        order = Order(
            external_id="EXT‑0001",
            marketplace=amazon,
            created_at=datetime.utcnow(),
            total_gross=Decimal("19.99"),
            delivery_cost=Decimal("3.99"),
            status="PAID",
            country="DE",
        )

        item = OrderItem(
            product=widget,
            order=order,          # attach via relationship
            price=Decimal("19.99"),
            quantity=1,
            currency="EUR",
        )

        session.add(order)       # cascades: item, product, marketplace
        session.commit()
        session.refresh(order)

        print(f"Inserted order id={order.id}")

if __name__ == "__main__":
    main()
