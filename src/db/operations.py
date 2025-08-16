from sqlmodel import Session
from src.db.engine import engine
from src.db.crud import (
    upsert_product,
    upsert_marketplace,
    get_or_create_offer_with_dependencies_efficient,
    get_or_create_order_with_dependencies_efficient,
    get_or_create_order_with_dependencies_parallel,
    create_stock_history_with_upsert_product,
)
from src.domain.entities import (
    Product,
    Marketplace,
    Offer,
    Order,
    ProductStock
)
from datetime import datetime


def bulk_upsert_products(products: list[Product]) -> int:
    """Bulk upsert products into database"""
    with Session(engine) as session:
        count = 0
        for product in products:
            upsert_product(session, product)
            count += 1
        session.commit()
    return count
    
    
def bulk_upsert_products_parallel(products_dicts: list[dict]) -> int:
    """Bulk upsert products into database"""
    with Session(engine) as session:
        count = 0
        for product_dict in products_dicts:
            product = Product.model_validate(product_dict)
            upsert_product(session, product)
            count += 1
        session.commit()
    return count



def bulk_upsert_marketplaces(marketplaces: list[Marketplace]) -> int:
    """Bulk upsert marketplaces into database"""
    with Session(engine) as session:
        count = 0
        for marketplace in marketplaces:
            upsert_marketplace(session, marketplace)
            count += 1
        session.commit()
    return count


def bulk_upsert_offers(offers: list[Offer]) -> int:
    """Bulk upsert offers with dependencies into database"""
    with Session(engine) as session:
        updated_offers = []
        for offer in offers:
            _, created = get_or_create_offer_with_dependencies_efficient(session, offer)
            updated_offers.append(not created)
        session.commit()
    return sum(updated_offers)


def bulk_upsert_offers_parallel(offers_dicts: list[dict]) -> int:
    """Bulk upsert offers with dependencies into database"""
    with Session(engine) as session:
        updated_offers = []
        for offer_dict in offers_dicts:
            offer = Offer.model_validate(offer_dict)
            _, created = get_or_create_offer_with_dependencies_efficient(session, offer)
            updated_offers.append(not created)
        session.commit()
    return sum(updated_offers)


def bulk_upsert_orders(orders: list[Order]) -> tuple[int, int]:
    """
    Bulk upsert orders with dependencies into database.
    Returns tuple of (total_processed, newly_created)
    """
    created_list = []
    with Session(engine) as session:
        for order in orders:
            _, was_created = get_or_create_order_with_dependencies_efficient(
                session=session, order_domain=order
            )
            created_list.append(was_created)
        session.commit()
    return sum(created_list)
    
    
def bulk_upsert_orders_parallel(order_domain_dicts: list[dict]):
    """
    Bulk upsert orders with dependencies into database.
    """
    created_list = []
    changed_list = []
    with Session(engine) as session:
        for order_dict in order_domain_dicts:
            order_domain = Order.model_validate(order_dict)
            _, was_created, _was_changed = get_or_create_order_with_dependencies_parallel(
                session=session, order_domain=order_domain
            )
            created_list.append(was_created)
            changed_list.append(_was_changed)
        session.commit()
    return sum(created_list), sum(changed_list)


def bulk_create_stock_history(products: list[ProductStock], date: datetime) -> int:
    """Bulk create stock history records"""
    with Session(engine) as session:
        for product in products:
            create_stock_history_with_upsert_product(session, product, date=date)
        session.commit()


def bulk_create_stock_history_parallel(products_dicts: list[dict], date: datetime) -> int:
    """Bulk create stock history records"""
    with Session(engine) as session:
        for product_dict in products_dicts:
            product = ProductStock.model_validate(product_dict)
            create_stock_history_with_upsert_product(session, product, date=date)
        session.commit()