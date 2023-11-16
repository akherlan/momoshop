# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.item import Item, Field


class BerrybenkaItem(Item):
    name = Field()
    price = Field()
    variant_name = Field()
    description = Field()
    category = Field()
    tag = Field()
    image = Field()
    brand = Field()
    slug = Field()
    source = Field()
    product_id = Field()
    referer = Field()
    date_acquisition = Field()

class GaudiItem(Item):
    product_id = Field()
    sku = Field()
    name = Field()
    brand = Field()
    category = Field()
    variant_id = Field()
    variant_name = Field()
    date_release = Field()
    description = Field()
    slug = Field()
    price = Field()
    is_instock = Field()
    date_acquisition = Field()
    source = Field()