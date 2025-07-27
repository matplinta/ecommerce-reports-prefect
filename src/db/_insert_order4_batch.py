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
from sqlmodel import Session, select

# -----------------------------------------------------------------------------
# assume: payloads is list[OrderCreate] (DTOs parsed from the API response)
# -----------------------------------------------------------------------------
def ingest_batch(payloads: list[OrderCreate], *, session: Session) -> None:

    # 1. Gather UNIQUE keys we’ll need
    want_market_names = {p.marketplace_name for p in payloads}
    want_skus         = {it.sku for p in payloads for it in p.items}

    # 2. Fetch what already exists with TWO queries
    markets = session.exec(
        select(Marketplace).where(Marketplace.name.in_(want_market_names))
    ).all()
    products = session.exec(
        select(Product).where(Product.sku.in_(want_skus))
    ).all()

    markets_by_name   = {m.name: m for m in markets}
    products_by_sku   = {p.sku: p for p in products}

    # 3. Build missing Marketplace & Product objects in memory
    new_markets  = [
        Marketplace(name=name)
        for name in want_market_names
        if name not in markets_by_name
    ]
    new_products = [
        Product(sku=sku, name="(unknown)")
        for sku in want_skus
        if sku not in products_by_sku
    ]
    session.bulk_save_objects(new_markets + new_products)
    session.flush()                          # now new rows have PKs

    # …update our lookup maps with freshly inserted rows
    for m in new_markets:
        markets_by_name[m.name] = m
    for p in new_products:
        products_by_sku[p.sku] = p

    # 4. Ensure (product, marketplace) links — collect UNIQUE pairs
    want_links = {
        (products_by_sku[it.sku].id, markets_by_name[p.marketplace_name].id)
        for p in payloads
        for it in p.items
    }

    existing_links = session.exec(
        select(ProductMarketplaceLink.product_id,
               ProductMarketplaceLink.marketplace_id)
        .where(
            tuple_(ProductMarketplaceLink.product_id,
                   ProductMarketplaceLink.marketplace_id).in_(want_links)
        )
    ).all()
    have_links = set(existing_links)

    link_objs = [
        ProductMarketplaceLink(product_id=pid, marketplace_id=mid)
        for pid, mid in want_links
        if (pid, mid) not in have_links
    ]
    session.bulk_save_objects(link_objs)
    session.flush()

    # 5. Build Order & OrderItem ORM rows
    order_rows: list[Order]       = []
    item_rows:  list[OrderItem]   = []

    for dto in payloads:
        mk = markets_by_name[dto.marketplace_name]
        order = Order(
            external_id=dto.external_id,
            created_at=dto.created_at or datetime.utcnow(),
            total_gross=dto.total_gross,
            delivery_cost=dto.delivery_cost,
            status=dto.status,
            country=dto.country,
            city=dto.city,
            marketplace_id=mk.id,
        )
        order_rows.append(order)

        for itm in dto.items:
            prod = products_by_sku[itm.sku]
            item_rows.append(
                OrderItem(
                    order=order,                 # will set FK after flush
                    product_id=prod.id,
                    price=itm.price,
                    quantity=itm.quantity,
                    currency=itm.currency,
                )
            )

    # 6. Bulk‑insert facts in ONE round‑trip
    session.bulk_save_objects(order_rows + item_rows)
    session.commit()


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


    order_db = ingest_batch(sample_payload)
    with Session(engine) as session:
        order_db = get_order(session, order_db.id)
        print(f"\n✅  Created order id={order_db.id} with {len(order_db.items)} item(s)")
    
    