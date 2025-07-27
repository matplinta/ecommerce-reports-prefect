from sqlmodel import Session, select
from sqlalchemy import and_
from decimal import Decimal
from datetime import datetime

from src.db.models import (
    Order,
    OrderItem,
    Product,
    Marketplace,
    PriceHistory,
    StockHistory,
    ProductMarketplaceLink,
)
from src.db.dto import (
    OrderCreate,
    OrderItemCreate,
    PriceHistoryCreate,
    ProductCreate,
    MarketplaceCreate,
    ProductMarketplaceLinkCreate,
    StockHistoryCreate,
)
from src.domain.entities import Order as OrderDomain


def get_or_create(session, model, defaults=None, **kwargs):
    """Get an instance by kwargs or create it if not exists."""
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


# --- Product CRUD ---


def create_product(*, session: Session, product_create: ProductCreate) -> Product:
    """Create a new Product."""
    db_obj = Product.model_validate(product_create)
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def get_product(session: Session, product_id: int) -> Product | None:
    """Retrieve a Product by its ID."""
    return session.get(Product, product_id)


def get_product_by_sku(session: Session, sku: str) -> Product | None:
    """Retrieve a Product by its SKU."""
    return session.exec(select(Product).where(Product.sku == sku)).first()


def get_products(session: Session) -> list[Product]:
    """Retrieve all Products."""
    return session.exec(select(Product)).all()


def delete_product(session: Session, product_id: int) -> None:
    """Delete a Product by its ID."""
    obj = session.get(Product, product_id)
    if obj:
        session.delete(obj)
        session.commit()


def get_or_create_product(session: Session, product_create: ProductCreate) -> Product:
    """Get or create a Product by SKU, updating name if needed."""
    prod = session.exec(
        select(Product).where(Product.sku == product_create.sku)
    ).first()
    if prod:
        # if prod.name != product_create.name:
        #     prod.name = product_create.name
        #     session.commit()
        return prod
    return create_product(session=session, product_create=product_create)


# --- Marketplace CRUD ---


def create_marketplace(
    *, session: Session, marketplace_create: MarketplaceCreate
) -> Marketplace:
    """Create a new Marketplace."""
    db_obj = Marketplace.model_validate(marketplace_create)
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def get_marketplace(session: Session, marketplace_id: int) -> Marketplace | None:
    """Retrieve a Marketplace by its ID."""
    return session.get(Marketplace, marketplace_id)


def get_marketplaces(session: Session) -> list[Marketplace]:
    """Retrieve all Marketplaces."""
    return session.exec(select(Marketplace)).all()


def delete_marketplace(session: Session, marketplace_id: int) -> None:
    """Delete a Marketplace by its ID."""
    obj = session.get(Marketplace, marketplace_id)
    if obj:
        session.delete(obj)
        session.commit()


def get_or_create_marketplace(
    session: Session, marketplace_create: MarketplaceCreate
) -> Marketplace:
    """Get or create a Marketplace by name and external_id."""
    mp = session.exec(
        select(Marketplace).where(
            and_(
                Marketplace.name == marketplace_create.name,
                Marketplace.external_id == marketplace_create.external_id,
            )
        )
    ).first()
    if mp:
        return mp
    return create_marketplace(session=session, marketplace_create=marketplace_create)


# --- ProductMarketplaceLink CRUD ---


def create_product_marketplace_link(
    *, session: Session, link_create: ProductMarketplaceLinkCreate
) -> ProductMarketplaceLink:
    """Create a ProductMarketplaceLink."""
    db_obj = ProductMarketplaceLink.model_validate(link_create)
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def get_product_marketplace_link(
    session: Session, product_id: int, marketplace_id: int
) -> ProductMarketplaceLink | None:
    """Retrieve a ProductMarketplaceLink by product and marketplace IDs."""
    return session.exec(
        select(ProductMarketplaceLink).where(
            and_(
                ProductMarketplaceLink.product_id == product_id,
                ProductMarketplaceLink.marketplace_id == marketplace_id,
            )
        )
    ).first()


def get_product_marketplace_links(session: Session) -> list[ProductMarketplaceLink]:
    """Retrieve all ProductMarketplaceLinks."""
    return session.exec(select(ProductMarketplaceLink)).all()


def delete_product_marketplace_link(
    session: Session, product_id: int, marketplace_id: int
) -> None:
    """Delete a ProductMarketplaceLink by product and marketplace IDs."""
    obj = get_product_marketplace_link(session, product_id, marketplace_id)
    if obj:
        session.delete(obj)
        session.commit()


def ensure_product_marketplace_link(
    session: Session, product_id: int, marketplace_id: int
) -> None:
    """Ensure a ProductMarketplaceLink exists for given product and marketplace."""
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


# --- Order CRUD ---


def create_order(*, session: Session, order_create: OrderCreate) -> Order:
    """Create a new Order."""
    db_obj = Order.model_validate(order_create)
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def get_or_create_order(*, session: Session, order_domain: OrderDomain) -> tuple[Order, bool]:
    """
    Create a single Order from domain schema, or return existing one.
    Returns (order, created: bool)
    """
    mp = get_or_create_marketplace(
        session, 
        MarketplaceCreate(
            external_id=order_domain.marketplace_extid,
            name=order_domain.marketplace_name,
        )
    )
    existing = order_exists(session, order_domain.external_id, mp.id)
    if existing:
        return existing, False


    order = create_order(
        session=session,
        order_create=OrderCreate(
            external_id=order_domain.external_id,
            created_at=order_domain.created_at,
            total_gross_original=Decimal(order_domain.total_gross_original),
            total_gross_pln=Decimal(order_domain.total_gross_pln),
            delivery_cost_original=Decimal(order_domain.delivery_cost_original),
            delivery_cost_pln=Decimal(order_domain.delivery_cost_pln),
            delivery_method=order_domain.delivery_method,
            currency=order_domain.currency,
            status=order_domain.status,
            country=order_domain.country,
            city=order_domain.city,
            marketplace_id=mp.id,
        ),
    )

    for it in order_domain.items:
        product = ProductCreate(sku=it.sku, name=it.name, price=it.price)
        prod = get_or_create_product(session, product)
        ensure_product_marketplace_link(session, prod.id, mp.id)

        order_item = create_order_item(
            session=session,
            order_item_create=OrderItemCreate(
                order_id=order.id,
                product_id=prod.id,
                price=Decimal(it.price),
                price_pln=Decimal(it.price_pln),
                quantity=it.quantity,
            ),
        )
        create_price_history(
            session=session,
            price_history_create=PriceHistoryCreate(
                product_id=prod.id,
                marketplace_id=mp.id,
                date=order.created_at,
                price_pln=Decimal(order_item.price_pln),
            ),
        )

    return order, True

def order_exists(session: Session, external_id: str, marketplace_id: int) -> bool:
    """Check if an order with the given external_id, marketplace_id, and created_at already exists."""
    stmt = select(Order).where(
        Order.external_id == external_id,
        Order.marketplace_id == marketplace_id
    )
    return session.exec(stmt).first() is not None


def get_order(session: Session, order_id: int) -> Order | None:
    """Retrieve an Order by its ID."""
    return session.get(Order, order_id)


def get_order_by_ext_id(session: Session, external_id: str) -> Order | None:
    """Retrieve an Order by its external ID."""
    return session.exec(select(Order).where(Order.external_id == external_id)).first()


def get_orders(session: Session) -> list[Order]:
    """Retrieve all Orders."""
    return session.exec(select(Order)).all()


def delete_order(session: Session, order_id: int) -> None:
    """Delete an Order by its ID."""
    obj = session.get(Order, order_id)
    if obj:
        session.delete(obj)
        session.commit()


# --- OrderItem CRUD ---


def create_order_item(
    *, session: Session, order_item_create: OrderItemCreate
) -> OrderItem:
    """Create a new OrderItem."""
    db_obj = OrderItem.model_validate(order_item_create)
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def get_order_item(session: Session, order_item_id: int) -> OrderItem | None:
    """Retrieve an OrderItem by its ID."""
    return session.get(OrderItem, order_item_id)


def get_order_items(session: Session) -> list[OrderItem]:
    """Retrieve all OrderItems."""
    return session.exec(select(OrderItem)).all()


def delete_order_item(session: Session, order_item_id: int) -> None:
    """Delete an OrderItem by its ID."""
    obj = session.get(OrderItem, order_item_id)
    if obj:
        session.delete(obj)
        session.commit()


# --- PriceHistory CRUD ---


def create_price_history(
    *, session: Session, price_history_create: PriceHistoryCreate
) -> PriceHistory:
    """Create a new PriceHistory entry."""
    db_obj = PriceHistory.model_validate(price_history_create)
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def get_price_history(session: Session, price_history_id: int) -> PriceHistory | None:
    """Retrieve a PriceHistory entry by its ID."""
    return session.get(PriceHistory, price_history_id)


def get_price_histories(session: Session) -> list[PriceHistory]:
    """Retrieve all PriceHistory entries."""
    return session.exec(select(PriceHistory)).all()


def delete_price_history(session: Session, price_history_id: int) -> None:
    """Delete a PriceHistory entry by its ID."""
    obj = session.get(PriceHistory, price_history_id)
    if obj:
        session.delete(obj)
        session.commit()


# --- StockHistory CRUD ---


def create_stock_history(
    *, session: Session, stock_history_create: StockHistoryCreate
) -> StockHistory:
    """Create a new StockHistory entry."""
    db_obj = StockHistory.model_validate(stock_history_create)
    session.add(db_obj)
    session.commit()
    session.refresh(db_obj)
    return db_obj


def get_stock_history(session: Session, stock_history_id: int) -> StockHistory | None:
    """Retrieve a StockHistory entry by its ID."""
    return session.get(StockHistory, stock_history_id)


def get_stock_histories(session: Session) -> list[StockHistory]:
    """Retrieve all StockHistory entries."""
    return session.exec(select(StockHistory)).all()


def delete_stock_history(session: Session, stock_history_id: int) -> None:
    """Delete a StockHistory entry by its ID."""
    obj = session.get(StockHistory, stock_history_id)
    if obj:
        session.delete(obj)
        session.commit()
