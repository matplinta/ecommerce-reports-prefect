from decimal import Decimal
from datetime import datetime

from sqlalchemy import Column, Numeric, UniqueConstraint, text
from sqlmodel import Field, Relationship, SQLModel, DateTime, Column


# ────────────────────────────────────────────
# Association table: many‑to‑many Product ↔ Marketplace
# ────────────────────────────────────────────
class ProductMarketplaceLink(SQLModel, table=True):
    __tablename__ = "product_marketplace"

    product_id: int = Field(
        foreign_key="product.id",
        primary_key=True,
        ondelete="CASCADE"
    )
    marketplace_id: int = Field(
        foreign_key="marketplace.id",
        primary_key=True,
        ondelete="CASCADE"
    )

# ────────────────────────────────────────────
# Core dimension tables
# ────────────────────────────────────────────
class Offer(SQLModel, table=True):
    __tablename__ = "offer"
    __table_args__ = (UniqueConstraint("external_id", "origin_id", "marketplace_id"),)

    id: int | None = Field(default=None, primary_key=True)
    external_id: str = Field(index=True)  # upstream identifier (listing ID from source)
    origin_id: str = Field()  # upstream identifier (listing ID from source marketplace)
    name: str = Field(max_length=255, nullable=False)
    started_at: datetime | None = Field(sa_column=Column(DateTime(timezone=True), nullable=True, index=True))
    ended_at: datetime | None = Field(sa_column=Column(DateTime(timezone=True), nullable=True, default=None))
    quantity_selling: int = Field(default=0, nullable=False)
    
    ean: str | None = Field(max_length=100, nullable=True)
    product_id: int = Field(foreign_key="product.id", index=True)
    marketplace_id: int = Field(foreign_key="marketplace.id", index=True, ondelete="CASCADE")

    status: str = Field(max_length=100, default="active")
    price_with_tax: Decimal = Field(default=0, max_digits=10, decimal_places=2, nullable=False)

    # relationships (optional)
    product: "Product" = Relationship(back_populates="offers")
    marketplace: "Marketplace" = Relationship(back_populates="offers")


class Marketplace(SQLModel, table=True):
    __tablename__ = "marketplace"
    __table_args__ = (UniqueConstraint("external_id", "type", "platform_origin"),)

    id: int | None = Field(default=None, primary_key=True)
    external_id: str = Field(default=None)
    platform_origin: str | None = Field(default=None, description="The origin of the marketplace e.g. 'Baselinker'")
    type: str | None = Field(default=None, description="Type of the marketplace platform (e.g., 'allegro', 'amazon')")
    name: str = Field(index=True)

    # back‑relationships
    products: list["Product"] = Relationship(
        back_populates="marketplaces",
        link_model=ProductMarketplaceLink,
    )
    orders: list["Order"] = Relationship(back_populates="marketplace")
    offers: list["Offer"] = Relationship(back_populates="marketplace")


class Product(SQLModel, table=True):
    __tablename__ = "product"

    id: int | None = Field(default=None, primary_key=True)
    sku: str = Field(index=True, unique=True)
    name: str = Field(max_length=255)
    image_url: str | None = Field(default=None, description="URL to the product image")
    kind: str | None = Field(default=None, description="Kind of the product (e.g., 'Komplet', 'Towar')")
    unit_purchase_cost: Decimal | None = Field(
        default=0, max_digits=10, decimal_places=2, description="Unit purchase cost of the product"
    )
    

    marketplaces: list[Marketplace] = Relationship(
        back_populates="products",
        link_model=ProductMarketplaceLink,
    )
    offers: list["Offer"] = Relationship(back_populates="product")
    price_history: list["PriceHistory"] = Relationship(back_populates="product")
    stock_history: list["StockHistory"] = Relationship(back_populates="product")


# ────────────────────────────────────────────
# Orders & items
# ────────────────────────────────────────────


class Order(SQLModel, table=True):
    __tablename__ = "order"
    __table_args__ = (UniqueConstraint("external_id", "marketplace_id"),)

    id: int | None = Field(default=None, primary_key=True)
    external_id: str = Field(index=True)
    created_at: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False, index=True))
    total_gross_original: Decimal = Field(
        default=0, max_digits=10, decimal_places=2, nullable=False
    )
    total_gross_pln: Decimal = Field(
        default=0, max_digits=10, decimal_places=2, nullable=False
    )
    delivery_cost_original: Decimal = Field(
        default=0, max_digits=10, decimal_places=2
    )
    delivery_cost_pln: Decimal = Field(
        default=0, max_digits=10, decimal_places=2
    )
    delivery_method: str | None = Field(max_length=255, default=None, description="Delivery method name")
    currency: str = Field(max_length=3, default="PLN", description="Currency of the price")
    status: str | None = Field(max_length=100)
    country: str | None = Field(max_length=100)
    city: str | None = Field(max_length=100)

    # FK → Marketplace
    marketplace_id: int = Field(foreign_key="marketplace.id", ondelete="CASCADE")
    marketplace: Marketplace = Relationship(back_populates="orders")

    # One‑to‑many order → items
    items: list["OrderItem"] = Relationship(back_populates="order")


class OrderItem(SQLModel, table=True):
    __tablename__ = "order_item"

    id: int | None = Field(default=None, primary_key=True)

    order_id: int = Field(foreign_key="order.id", ondelete="CASCADE")
    product_id: int = Field(foreign_key="product.id")

    price: Decimal = Field(default=0, max_digits=10, decimal_places=2, nullable=False, description="Gross price per item at the time of order")
    price_pln: Decimal = Field(default=0, max_digits=10, decimal_places=2, nullable=False, description="Gross price in PLN per item at the time of order")
    quantity: int = Field(default=1)
    tax_rate: Decimal = Field(
        default=Decimal("23"),
        sa_column=Column(Numeric(5, 2), server_default=text("23"), nullable=False),
        description="VAT rate in percent (e.g., 23 = 23%)",
    )


    order: Order = Relationship(back_populates="items")
    product: Product = Relationship()


# ────────────────────────────────────────────
# Time‑series snapshot tables
# ────────────────────────────────────────────
class PriceHistory(SQLModel, table=True):
    __tablename__ = "price_history"
    # __table_args__ = (UniqueConstraint("product_id", "marketplace_id", "date"),)

    id: int | None = Field(default=None, primary_key=True)

    product_id: int = Field(foreign_key="product.id", ondelete="CASCADE")
    marketplace_id: int = Field(foreign_key="marketplace.id", ondelete="CASCADE")
    date: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False, index=True))
    price_pln: Decimal = Field(default=0, max_digits=10, decimal_places=2, nullable=False)

    product: Product = Relationship(back_populates="price_history")
    marketplace: Marketplace = Relationship()


class StockHistory(SQLModel, table=True):
    __tablename__ = "stock_history"
    __table_args__ = (UniqueConstraint("product_id", "date"),)

    id: int | None = Field(default=None, primary_key=True)
    product_id: int = Field(foreign_key="product.id", ondelete="CASCADE")
    date: datetime = Field(sa_column=Column(DateTime(timezone=True), nullable=False, index=True))
    stock: int

    product: Product = Relationship(back_populates="stock_history")
