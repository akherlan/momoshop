from pydantic import BaseModel, Field
from typing import List, Optional
import datetime


class Product(BaseModel):
    product_id: str
    name: str
    brand: str
    category: str = None
    tag: str = None
    description: str = None
    date_published: str = None
    slug: str = None
    variants: Optional[List[str]] = Field(default_factory=list)


class Variant(BaseModel):
    variant_id: str
    value: str = Field(default=None, validation_alias="variants")
    name: str = Field(default=None, validation_alias="variant_category")


class Price(BaseModel):
    price_id: str
    product_id: str
    variant_id: str
    price: int
    sku: str = None
    date_acquisition: str = None
    source: str = None


class Inventory(BaseModel):
    inventory_id: str
    product_id: str
    variant_id: str
    is_instock: bool = Field(default=True, validation_alias="stock")
    quantity: int = None
    condition: str = Field(default="new")
    sku: str = None
    modified_at: str = Field(default=None, validation_alias="date_acquisition")
    source: str = None