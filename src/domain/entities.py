from decimal import Decimal
from datetime import datetime
from pydantic import BaseModel, Field, field_validator, computed_field


OFFER_STATUS_MAP = {
    2: "Active",              # Aktywna
    66: "Creating (errored)", # Tworzenie
    67: "Creating",           # Tworzenie
    80: "Ended",              # Zakończona
    81: "Ended (No status)",  # Zakończona (brak stanu)
    82: "Ended (manually)",   # Zakończona (ręcznie)
    83: "Ended (naturally)",  # Zakończona (naturalnie)
    89: "Archived",           # Archiwum
}

class Marketplace(BaseModel):
    external_id: str
    platform_origin: str 
    type: str
    name: str

class Product(BaseModel):
    sku: str
    name: str | None = None
    image_url: str | None = None

class Offer(BaseModel):
    external_id: str # check if not null
    name: str
    started_at: datetime
    ended_at: datetime | None = None
    quantity_selling: int
    sku: str
    ean: str | None = None
    marketplace_extid: str
    platform_origin: str 
    type: str
    price_with_tax: Decimal
    status_id: int
    
    
    @computed_field
    @property
    def status_name(self) -> str:
        return OFFER_STATUS_MAP.get(self.status_id, 'unknown')

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