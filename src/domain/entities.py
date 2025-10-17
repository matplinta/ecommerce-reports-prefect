from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field, field_validator


class Marketplace(BaseModel):
    external_id: str
    platform_origin: str 
    type: str
    name: str

class Product(BaseModel):
    sku: str
    name: str | None = None
    image_url: str | None = None
    kind: str | None = None
    unit_purchase_cost: Decimal | None = None
    
    @field_validator("name")
    @classmethod
    def replace_single_quote_with_double(cls, v: str | None) -> str | None:
        if v is not None:
            return v.replace("'", '"')
        return v
    
class ProductStock(Product):
    stock: int

class Offer(BaseModel):
    external_id: str # check if not null
    origin_id: str # check if not null
    name: str
    started_at: datetime | None = None
    ended_at: datetime | None = None
    quantity_selling: int
    sku: str
    ean: str | None = None
    marketplace_extid: str
    platform_origin: str 
    marketplace_type: str
    marketplace_name: str
    price_with_tax: Decimal
    status_id: int
    status_name: str
    is_active: bool

    @field_validator("price_with_tax")
    @classmethod
    def round_to_2_decimal_places(cls, v: Decimal) -> Decimal:
        return Decimal(v).quantize(Decimal('0.01'))

class OrderItem(BaseModel):
    sku: str
    name: str
    price: Decimal
    price_pln: Decimal
    quantity: int
    tax_rate: Decimal
    
    @field_validator("price", "price_pln", "tax_rate")
    @classmethod
    def round_to_2_decimal_places(cls, v: Decimal) -> Decimal:
        return Decimal(v).quantize(Decimal('0.01'))
    

class Order(BaseModel):
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
    ignore: bool = False
    created_at: datetime
    marketplace_extid: str
    marketplace_name: str
    platform_origin: str 
    marketplace_type: str
    items: list[OrderItem] = Field(default_factory=list)

    # @field_validator("city")
    # @classmethod
    # def capitalize_city(cls, v: str) -> str:
    #     return v.capitalize()

    @field_validator("currency")
    @classmethod
    def upper_currency(cls, v: str) -> str:
        return v.upper()
    
    @field_validator("total_gross_original", "total_gross_pln", "delivery_cost_original", "delivery_cost_pln")
    @classmethod
    def round_to_2_decimal_places(cls, v: Decimal) -> Decimal:
        return Decimal(v).quantize(Decimal('0.01'))