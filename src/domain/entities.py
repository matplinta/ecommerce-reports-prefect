from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field, ConfigDict, field_validator


class OrderItem(BaseModel):
    sku: str
    name: str
    price: Decimal
    price_pln: Decimal
    quantity: int
    
    @field_validator("price", "price_pln")
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
    created_at: datetime
    marketplace_extid: str
    marketplace_name: str
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