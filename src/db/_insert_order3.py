from datetime import datetime
from decimal import Decimal

from sqlmodel import Session, select

from src.db.models import Marketplace, Product, Order, OrderItem, ProductMarketplaceLink
from src.db.dto import OrderCreate, OrderItemCreate
from src.db.crud import (
    get_marketplace, create_marketplace,
    get_product, create_product,
    create_order, create_order_item, get_order
)
from src.db.engine import engine

def get_or_create_marketplace(session: Session, name: str) -> Marketplace:
    mp = session.exec(select(Marketplace).where(Marketplace.name == name)).first()
    if mp:
        return mp
    mp = Marketplace(name=name)
    session.add(mp)
    session.commit()
    session.refresh(mp)
    return mp


def get_or_create_product(session: Session, sku: str, name: str) -> Product:
    prod = session.exec(select(Product).where(Product.sku == sku)).first()
    if prod:
        return prod
    prod = Product(sku=sku, name=name)
    session.add(prod)
    session.commit()
    session.refresh(prod)
    return prod


def ensure_product_marketplace_link(
    session: Session, product_id: int, marketplace_id: int
) -> None:
    exists = session.exec(
        select(ProductMarketplaceLink).where(
            ProductMarketplaceLink.product_id == product_id,
            ProductMarketplaceLink.marketplace_id == marketplace_id,
        )
    ).first()
    if not exists:
        session.add(
            ProductMarketplaceLink(
                product_id=product_id,
                marketplace_id=marketplace_id,
            )
        )
        session.commit()


# ───────────────────────────────────────────────────────────
# main creator
# ───────────────────────────────────────────────────────────
def create_single_order(order_dict: dict) -> Order:
    """Validate payload → insert one order with its items. Returns DB object."""
    dto = OrderCreate.model_validate(order_dict)

    with Session(engine) as session:
        # 1. marketplace
        mp = get_or_create_marketplace(session, dto.marketplace_name)

        # 2. Order row
        order = Order(
            external_id=dto.external_id,
            created_at=dto.created_at or datetime.utcnow(),
            total_gross=Decimal(dto.total_gross),
            delivery_cost=Decimal(dto.delivery_cost),
            status=dto.status,
            country=dto.country,
            city=dto.city,
            marketplace_id=mp.id,
        )
        session.add(order)
        session.commit()          # assigns order.id
        session.refresh(order)

        # 3. items
        for it in dto.items:
            prod = get_or_create_product(session, it.sku, it.name)
            ensure_product_marketplace_link(session, prod.id, mp.id)

            item_row = OrderItem(
                order_id=order.id,
                product_id=prod.id,
                price=Decimal(it.price),
                quantity=it.quantity,
                currency=it.currency,
            )
            session.add(item_row)

        session.commit()
        session.refresh(order)
        return order


# ───────────────────────────────────────────────────────────
# quick demo payload & run
# ───────────────────────────────────────────────────────────
if __name__ == "__main__":
    sample_payload = {
        "external_id": "AWDAWD123",
        "total_gross": "42.50",
        "delivery_cost": "5.49",
        "status": "SHIPPED",
        "country": "CZ",
        "city": "Prague",
        "marketplace_name": "Allegro",
        "items": [
            {
                "sku": "XYZ789",
                "name": "Blue Gadget",
                "price": "42.50",
                "quantity": 1,
                "currency": "CZK"
            }
        ]
    }


    order_db = create_single_order(sample_payload)
    with Session(engine) as session:
        order_db = get_order(session, order_db.id)
        print(f"\n✅  Created order id={order_db.id} with {len(order_db.items)} item(s)")
    
    