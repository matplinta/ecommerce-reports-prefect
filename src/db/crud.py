from datetime import datetime
from decimal import Decimal

from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session, select

from src.db.dto import (
    MarketplaceCreate,
    OfferCreate,
    OrderCreate,
    OrderItemCreate,
    PriceHistoryCreate,
    ProductCreate,
    ProductMarketplaceLinkCreate,
    StockHistoryCreate,
)
from src.db.models import (
    Marketplace,
    Offer,
    Order,
    OrderItem,
    PriceHistory,
    Product,
    ProductMarketplaceLink,
    StockHistory,
)
from src.domain.entities import Marketplace as MarketplaceDomain
from src.domain.entities import Offer as OfferDomain
from src.domain.entities import Order as OrderDomain
from src.domain.entities import Product as ProductDomain
from src.domain.entities import ProductStock as ProductStockDomain


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


def upsert_product(
    session: Session, product_domain: ProductDomain, name_overwrite: bool = True
) -> Product:
    values = product_domain.model_dump(exclude_unset=True)
    to_overwrite = {k: v for k, v in values.items() if k != "sku"}
    if name_overwrite is False:
        to_overwrite.pop("name", None)

    stmt = insert(Product).values(**values)
    stmt = stmt.on_conflict_do_update(index_elements=["sku"], set_=to_overwrite)
    session.exec(stmt)
    return session.exec(
        select(Product).where(Product.sku == product_domain.sku)
    ).first()


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
                Marketplace.external_id == marketplace_create.external_id,
                Marketplace.platform_origin == marketplace_create.platform_origin,
                Marketplace.type == marketplace_create.type,
            )
        )
    ).first()
    if mp:
        return mp
    return create_marketplace(session=session, marketplace_create=marketplace_create)


def upsert_marketplace(
    session: Session, marketplace_domain: MarketplaceDomain
) -> Marketplace:
    stmt = insert(Marketplace).values(
        **marketplace_domain.model_dump(exclude_unset=True)
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["external_id", "platform_origin", "type"],
        set_={"name": marketplace_domain.name},
    )
    session.exec(stmt)
    return session.exec(
        select(Marketplace).where(
            Marketplace.external_id == marketplace_domain.external_id,
            Marketplace.platform_origin == marketplace_domain.platform_origin,
            Marketplace.type == marketplace_domain.type,
        )
    ).first()


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


def get_or_create_order_with_dependencies(
    *, session: Session, order_domain: OrderDomain
) -> tuple[Order, bool]:
    """
    Create a single Order from domain schema, or return existing one.
    Returns (order, created: bool)
    """
    mp = get_or_create_marketplace(
        session,
        MarketplaceCreate(
            external_id=order_domain.marketplace_extid,
            name=order_domain.marketplace_name,
            platform_origin=order_domain.platform_origin,
            type=order_domain.marketplace_type,
        ),
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

    return order, True


def get_or_create_order_with_dependencies_efficient(
    *, session: Session, order_domain: OrderDomain
) -> tuple[Order, bool]:
    """
    Efficiently create a single Order from domain schema, or return existing one.
    """
    # 1. Marketplace
    mp = session.exec(
        select(Marketplace).where(
            Marketplace.external_id == order_domain.marketplace_extid,
            Marketplace.name == order_domain.marketplace_name,
            Marketplace.type == order_domain.marketplace_type,
            Marketplace.platform_origin == order_domain.platform_origin,
        )
    ).first()
    if not mp:
        mp = Marketplace(
            external_id=order_domain.marketplace_extid,
            name=order_domain.marketplace_name,
            type=order_domain.marketplace_type,
            platform_origin=order_domain.platform_origin,
        )
        session.add(mp)
        session.flush()  # assign id without commit

    # 2. Order exists?
    existing_order = session.exec(
        select(Order).where(
            Order.external_id == order_domain.external_id,
            Order.marketplace_id == mp.id,
        )
    ).first()
    if existing_order:
        return existing_order, False

    # 3. Products: batch get/create
    skus = [it.sku for it in order_domain.items]
    existing_products = {
        p.sku: p
        for p in session.exec(select(Product).where(Product.sku.in_(skus))).all()
    }
    new_products = []
    for it in order_domain.items:
        if it.sku not in existing_products:
            prod = Product(sku=it.sku, name=it.name)
            session.add(prod)
            new_products.append(prod)
    session.flush()  # assign ids to new products

    # Refresh product dict with new products
    for prod in new_products:
        existing_products[prod.sku] = prod

    # 4. Order
    order = Order(
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
    )
    session.add(order)
    session.flush()  # assign id

    # 5. ProductMarketplaceLinks: batch get/create
    existing_links = {
        (link.product_id, link.marketplace_id)
        for link in session.exec(
            select(ProductMarketplaceLink).where(
                ProductMarketplaceLink.product_id.in_(
                    [p.id for p in existing_products.values()]
                ),
                ProductMarketplaceLink.marketplace_id == mp.id,
            )
        ).all()
    }
    for prod in existing_products.values():
        key = (prod.id, mp.id)
        if key not in existing_links:
            session.add(
                ProductMarketplaceLink(product_id=prod.id, marketplace_id=mp.id)
            )

    # 6. OrderItems: batch add
    for it in order_domain.items:
        prod = existing_products[it.sku]
        order_item = OrderItem(
            order_id=order.id,
            product_id=prod.id,
            price=Decimal(it.price),
            price_pln=Decimal(it.price_pln),
            quantity=it.quantity,
        )
        session.add(order_item)

    session.commit()
    session.refresh(order)
    return order, True


def upsert_product_old(session, sku, name):
    stmt = insert(Product).values(sku=sku, name=name)
    stmt = stmt.on_conflict_do_nothing(index_elements=["sku"])
    session.execute(stmt)
    # Fetch the product (either just inserted or already existing)
    return session.exec(select(Product).where(Product.sku == sku)).first()


def upsert_marketplace_old(
    session, external_id, name, platform_origin, marketplace_type
):
    stmt = insert(Marketplace).values(
        external_id=external_id,
        name=name,
        platform_origin=platform_origin,
        type=marketplace_type,
    )
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["external_id", "platform_origin", "type"]
    )
    session.execute(stmt)
    return session.exec(
        select(Marketplace).where(
            and_(
                Marketplace.external_id == external_id,
                Marketplace.name == name,
                Marketplace.platform_origin == platform_origin,
                Marketplace.type == marketplace_type,
            )
        )
    ).first()


def upsert_product_marketplace_link(session, product_id, marketplace_id):
    stmt = insert(ProductMarketplaceLink).values(
        product_id=product_id, marketplace_id=marketplace_id
    )
    stmt = stmt.on_conflict_do_nothing(index_elements=["product_id", "marketplace_id"])
    session.execute(stmt)
    # Optionally fetch the link (either just inserted or already existing)
    return session.exec(
        select(ProductMarketplaceLink).where(
            ProductMarketplaceLink.product_id == product_id,
            ProductMarketplaceLink.marketplace_id == marketplace_id,
        )
    ).first()


def get_or_create_order_with_dependencies_parallel(
    *, session: Session, order_domain: OrderDomain
) -> tuple[Order, bool]:
    """
    Efficiently create a single Order from domain schema, or return existing one.
    Returns (order, created: bool)
    """
    # 1. Marketplace (upsert)
    mp = upsert_marketplace_old(
        session,
        external_id=order_domain.marketplace_extid,
        name=order_domain.marketplace_name,
        platform_origin=order_domain.platform_origin,
        marketplace_type=order_domain.marketplace_type,
    )

    # 2. Order exists?
    existing_order = session.exec(
        select(Order).where(
            Order.external_id == order_domain.external_id,
            Order.marketplace_id == mp.id,
        )
    ).first()
    if existing_order:
        return existing_order, False

    # 3. Products: batch upsert
    skus = [it.sku for it in order_domain.items]
    existing_products = {
        p.sku: p
        for p in session.exec(select(Product).where(Product.sku.in_(skus))).all()
    }
    for it in order_domain.items:
        if it.sku not in existing_products:
            prod = upsert_product_old(session, it.sku, it.name)
            existing_products[it.sku] = prod

    # 4. Order
    order = Order(
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
    )
    session.add(order)
    session.flush()  # assign id

    for prod in existing_products.values():
        upsert_product_marketplace_link(session, prod.id, mp.id)

    # 6. OrderItems and PriceHistory: batch add
    for it in order_domain.items:
        prod = existing_products[it.sku]
        order_item = OrderItem(
            order_id=order.id,
            product_id=prod.id,
            price=Decimal(it.price),
            price_pln=Decimal(it.price_pln),
            quantity=it.quantity,
        )
        session.add(order_item)

    session.commit()
    session.refresh(order)
    return order, True


def order_exists(session: Session, external_id: str, marketplace_id: int) -> bool:
    """Check if an order with the given external_id, marketplace_id, and created_at already exists."""
    stmt = select(Order).where(
        Order.external_id == external_id, Order.marketplace_id == marketplace_id
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


def create_stock_history_with_upsert_product(
    session: Session, product_stock: ProductStockDomain, date: datetime = None
) -> tuple[Product, StockHistory]:
    """
    Upserts a product and creates a stock history entry for the given date.

    Args:
        session: SQLModel Session
        product_data: Dict with product details (sku, name, kind, stock, unit_purchase_cost)
        date: Date for stock history (defaults to current date)

    Returns:
        Tuple of (product, stock_history)
    """
    if date is None:
        date = datetime.now().date()

    product_domain = ProductDomain(
        sku=product_stock.sku,
        name=product_stock.name,
        kind=product_stock.kind,
        unit_purchase_cost=product_stock.unit_purchase_cost,
    )
    product = upsert_product(session, product_domain, name_overwrite=False)

    # 2. Create stock history
    stmt = insert(StockHistory).values(
        product_id=product.id, date=date, stock=product_stock.stock
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=["product_id", "date"], set_={"stock": product_stock.stock}
    )
    session.exec(stmt)

    # 3. Get the stock history entry
    stock_history = session.exec(
        select(StockHistory).where(
            StockHistory.product_id == product.id, StockHistory.date == date
        )
    ).first()

    return product, stock_history


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


def get_or_create_offer_with_dependencies_efficient(
    session: Session, offer_domain: OfferDomain
) -> tuple[Offer, bool]:
    """
    Efficiently create a single Offer from domain schema, or update if it exists.
    Returns (offer, created: bool)
    """
    # 1. Marketplace (upsert)
    mp = upsert_marketplace_old(
        session,
        external_id=offer_domain.marketplace_extid,
        name=offer_domain.marketplace_name,
        platform_origin=offer_domain.platform_origin,
        marketplace_type=offer_domain.marketplace_type,
    )

    # 2. Product (upsert)
    prod = upsert_product_old(session, offer_domain.sku, offer_domain.name)
    session.flush()
    # 3. Ensure product-marketplace link
    upsert_product_marketplace_link(session, prod.id, mp.id)

    # 4. Check if offer exists
    existing_offer = session.exec(
        select(Offer).where(
            Offer.external_id == offer_domain.external_id,
            Offer.marketplace_id == mp.id,
        )
    ).first()

    if existing_offer:
        # Update fields that might have changed
        existing_offer.name = offer_domain.name
        existing_offer.ean = offer_domain.ean
        existing_offer.started_at = offer_domain.started_at
        existing_offer.ended_at = offer_domain.ended_at
        existing_offer.quantity_selling = offer_domain.quantity_selling
        existing_offer.price_with_tax = Decimal(offer_domain.price_with_tax)
        existing_offer.status = offer_domain.status_name
        existing_offer.product_id = prod.id  # Ensure correct product relation

        session.commit()
        # session.flush()  # Ensure existing_offer.id is set
        session.refresh(existing_offer)
        
        if offer_domain.is_active:
            price_history = PriceHistory(
                product_id=prod.id,
                marketplace_id=mp.id,
                date=datetime.now(),
                price_pln=Decimal(offer_domain.price_with_tax),
            )
            session.add(price_history)
        return existing_offer, False

    # 5. Create new offer
    new_offer = Offer(
        external_id=offer_domain.external_id,
        name=offer_domain.name,
        started_at=offer_domain.started_at,
        ended_at=offer_domain.ended_at,
        quantity_selling=offer_domain.quantity_selling,
        ean=offer_domain.ean,
        price_with_tax=Decimal(offer_domain.price_with_tax),
        status=offer_domain.status_name,
        marketplace_id=mp.id,
        product_id=prod.id,
    )
    session.add(new_offer)

    if offer_domain.is_active:
        price_history = PriceHistory(
            product_id=prod.id,
            marketplace_id=mp.id,
            date=datetime.now(),
            price_pln=Decimal(offer_domain.price_with_tax),
        )
        session.add(price_history)

    session.commit()
    session.refresh(new_offer)
    # session.flush()  # Ensure new_offer.id is set
    return new_offer, True
