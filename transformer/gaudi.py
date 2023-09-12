import sys
import os
import glob
import json

import datetime

from .file import FileHandler
from .dataclass import Product, Price, Inventory, Variant


def process(path:str, pattern:str, name:str) -> None:
    fh = FileHandler(name)
    master = fh.load(path, pattern)
    path_data_master = os.path.join(path, "{}.json".format(name))

    # create master table
    variant_name = list()
    for cell in master.variant_name.tolist():
        variant_name.append([x.strip() for x in cell.split(",")])
    master["variants"] = variant_name
    master["product_id"] = [str(x) for x in master["product_id"].tolist()]
    master = master.loc[master.astype(str).drop_duplicates(["product_id", "slug", "variants"]).index]
    fh.export(master, path_data_master)
    
    # expand variants to generate variant_id
    variants = master.explode("variants", ignore_index=True)
    variant_name = variants["variants"].tolist()
    variants["variant_id"] = [
        "{}-var-{}".format(name, variant_name[i].lower().replace(" ", "-"))
        for i in range(len(variants.index))
    ]
    variants = variants.drop_duplicates(ignore_index=True)

    # create price_history table
    # important! must have date_acquisition, price, and source
    price_cols = ['product_id', 'variant_id', 'sku', 'price', 'date_acquisition', 'source']
    prices = variants[price_cols].copy()
    datestrings = prices["date_acquisition"].to_list()
    def dt_convert(dt:str, fmt:str) -> datetime.datetime:
        t = datetime.datetime.strptime(''.join(dt.rsplit(':', 1)), fmt)
        return t.timestamp()
    datestrings_int = [
        str(int(dt_convert(t, '%Y-%m-%dT%H:%M:%S%z')))
        for t in datestrings
    ]
    product_id = prices["product_id"].tolist()
    variant_id = prices["variant_id"].tolist()
    prices["price_id"] = [
        "-".join([datestrings_int[x], product_id[x], variant_id[x]])
        for x in range(len(prices.index))
    ]
    path_data_price = fh.create_path(path, table="prices")
    fh.export(prices, path_data_price)
    fh.reshape(datacls=Price, obj=path_data_price, file=path_data_price)

    # create inventory table
    # important! sort from the lastest update modified (acquisition) date
    invent_cols = ["product_id", "variant_id", "is_instock", "quantity", "condition", "sku", "modified_at", "source", "date_acquisition"]
    invent_cols = [col for col in invent_cols if col in variants.columns.tolist()]
    inventory = variants[invent_cols].copy()
    inventory = inventory.sort_values(["date_acquisition", "product_id", "variant_id"], ascending=[False, True, True])
    inventory = inventory.drop_duplicates(["product_id", "variant_id", "sku"], ignore_index=True)
    invhead = inventory["product_id"].tolist()
    invtail = inventory["variant_id"].tolist()
    inventory["inventory_id"] = [
        "{}-{}".format(invhead[i], invtail[i])
        for i in range(len(inventory.index))
    ]
    path_data_inventory = fh.create_path(path, table="inventory")
    fh.export(inventory, path_data_inventory)
    fh.reshape(datacls=Inventory, obj=path_data_inventory, file=path_data_inventory)

    # create variant table
    variant_cols = ["variant_id", "variants"]
    variants = variants[variant_cols]
    variants = variants.drop_duplicates(ignore_index=True)
    path_data_variant = fh.create_path(path, table="variants")
    fh.export(variants, path_data_variant)
    fh.reshape(datacls=Variant, obj=path_data_variant, file=path_data_variant)

    # create product table
    path_data_product = fh.create_path(path, table="products")
    fh.reshape(datacls=Product, obj=path_data_master, file=path_data_product)