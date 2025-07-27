from sqlmodel import Field, SQLModel
from decimal import Decimal
from datetime import datetime


class ProductCreate(SQLModel):
    sku: str = Field(nullable=False)
    name: str


class MarketplaceCreate(SQLModel):
    external_id: str
    name: str


class ProductMarketplaceLinkCreate(SQLModel):
    product_id: int
    marketplace_id: int


class PriceHistoryCreate(SQLModel):
    product_id: int
    marketplace_id: int
    date: datetime
    price_pln: Decimal


class StockHistoryCreate(SQLModel):
    product_id: int
    date: datetime
    stock: int


class OrderItemCreate(SQLModel):
    order_id: int
    product_id: int
    price: Decimal
    price_pln: Decimal
    quantity: int


class OrderCreate(SQLModel):
    external_id: str
    total_gross_original: Decimal
    total_gross_pln: Decimal
    delivery_cost_original: Decimal
    delivery_cost_pln: Decimal
    delivery_method: str | None = None
    currency: str
    status: str
    country: str | None = None
    city: str | None = None
    created_at: datetime
    marketplace_id: int
