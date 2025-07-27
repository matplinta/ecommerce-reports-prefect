from datetime import datetime
from decimal import Decimal

from sqlmodel import Session, select

from src.db.models import Marketplace, Product, Order, OrderItem, ProductMarketplaceLink
from src.db.dto import OrderCreate, OrderItemCreate
from src.domain import OrderCanonical, OrderItemCanonical

from src.db.crud import (
    create_single_order
)
from src.db.engine import engine



if __name__ == "__main__":
    # sample_payload = {
    #     "external_id": "AWDAWD123",
    #     "total_gross": "42.50",
    #     "delivery_cost": "5.49",
    #     "status": "SHIPPED",
    #     "country": "CZ",
    #     "created_at": "2025-03-15T12:34:56",
    #     "city": "Prague",
    #     "marketplace_extid": "123",
    #     "marketplace_name": "Example Marketplace",
    #     "items": [
    #         {
    #             "sku": "XYZ789",
    #             "name": "Blue Gadget",
    #             "price": "42.50",
    #             "quantity": 1,
    #             "currency": "CZK",
    #         }
    #     ]
    # }

    sample_payload = OrderCanonical(
        external_id="AWDAWD11111",
        total_gross=42.50,
        delivery_cost=5.49,
        status="SHIPPED",
        country="CZ",
        city="Prague",
        created_at=datetime.fromisoformat("2025-03-15T12:34:56"),
        marketplace_extid=123,
        marketplace_name="Example Marketplace",
        items=[
            OrderItemCanonical(
                sku="XYZ789",
                name="Blue Gadget",
                price=42.50,
                price_pln=42.50,
                quantity=1,
                currency="CZK"
            )
        ]
    )

    
    with Session(engine) as session:
        order_db = create_single_order(session=session, order_domain=sample_payload)
        print(f"\n✅  Created order id={order_db.id} with {len(order_db.items)} item(s)")
    
    