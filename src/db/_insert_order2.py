from datetime import datetime
from decimal import Decimal

from sqlmodel import Session

from src.db.models import Marketplace, Product, Order, OrderItem
from src.db.dto import OrderCreate, OrderItemCreate
from src.db.crud import (
    get_marketplace, create_marketplace,
    get_product, create_product,
    create_order, create_order_item
)
from src.db.engine import engine

def get_or_create_marketplace(session: Session, name: str) -> Marketplace:
    # Try to get by name, else create
    stmt = Marketplace.__table__.select().where(Marketplace.name == name)
    result = session.exec(stmt).first()
    if result:
        return result
    return create_marketplace(session=session, marketplace_create=Marketplace(name=name))

def get_or_create_product(session: Session, sku: str, name: str) -> Product:
    stmt = Product.__table__.select().where(Product.sku == sku)
    result = session.exec(stmt).first()
    if result:
        return result
    return create_product(session=session, product_create=Product(sku=sku, name=name))

def main():
    with Session(engine) as session:
        # 1. marketplace (idempotent)
        empik = get_or_create_marketplace(session, name="Empik")
        print(f"Marketplace: {empik}, id: {empik.id}")

        # 2. product (idempotent)
        widget = get_or_create_product(session, sku="X123", name="Blue Widget")

        # 3. assemble order + item
        order = OrderCreate(
            external_id="EXT22221",
            created_at=datetime.utcnow(),
            total_gross=Decimal("19.99"),
            delivery_cost=Decimal("3.99"),
            status="PAID",
            country="PL",
            city="Warsaw",
            marketplace_id=empik.id
        )
        order = create_order(session=session, order_create=order)

        item = OrderItemCreate(
            product_id=widget.id,
            order_id=order.id,
            price=Decimal("19.99"),
            quantity=1,
            currency="EUR",
        )
        create_order_item(session=session, order_item_create=item)

        print(f"Inserted order id={order.id}")

if __name__ == "__main__":
    main()
