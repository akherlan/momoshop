#!/usr/bin/env python3

import pandas as pd

import os
import glob


def import_data(paths: list):
    data = [pd.read_csv(file, header=None) for file in paths]
    fields_len = len(data[0].columns)
    if not sum([len(p.columns) != fields_len for p in data]):
        data = pd.concat(data, ignore_index=True)
        data = data.drop_duplicates(ignore_index=True)
        if len(data.columns) == 10:  # products
            data.columns = [
                "product_id",
                "sku",
                "name",
                "brand",
                "category",
                "variant_id",
                "variant_name",
                "date_published",
                "description",
                "slug",
            ]
        if len(data.columns) == 7:  # offers
            data.columns = [
                "product_id",
                "variant_id",
                "sku",
                "price",
                "is_instock",
                "date_acquisition",
                "source",
            ]
        return data
    else:
        raise Exception("different columns length")


def extract_dataset(data, name: str):
    if name not in ("product", "variant", "price"):
        return "Error: invalid name"
    if name == "product":
        fields_name = [
            "product_id",
            "name",
            "brand",
            "category",
            "date_published",
            "slug",
        ]
        fields_unique = ["product_id", "name", "slug"]
        fields_sort = ["product_id", "slug"]
    if name == "variant":
        fields_name = ["product_id", "sku", "variant_id", "variant_name"]
        fields_unique = ["sku", "variant_id", "variant_name"]
        fields_sort = ["product_id", "sku", "variant_id"]
    if name == "price":
        fields_name = [
            "product_id",
            "variant_id",
            "sku",
            "price",
            "is_instock",
            "date_acquisition",
            "source",
        ]
        fields_unique = [
            "product_id",
            "variant_id",
            "sku",
            "date_acquisition",
            "source",
        ]
        fields_sort = ["product_id", "sku", "variant_id", "date_acquisition", "source"]
    return (
        data[fields_name]
        .drop_duplicates(fields_unique)
        .sort_values(fields_sort, ignore_index=True)
    )


if __name__ == "__main__":
    product_paths = glob.glob("data/products_[!a-z]*.csv")
    offer_paths = glob.glob("data/offers_[!a-z]*.csv")

    data_products = import_data(product_paths)
    data_pricing = import_data(offer_paths)

    products = extract_dataset(data_products, name="product")
    variants = extract_dataset(data_products, name="variant")
    prices = extract_dataset(data_pricing, name="price")

    products.to_csv("data/products_concat.csv", index=False)
    variants.to_csv("data/variants_concat.csv", index=False)
    prices.to_csv("data/prices_concat.csv", index=False)

    for f in product_paths + offer_paths:
        os.remove(f)
        print(f"delete {f}")
