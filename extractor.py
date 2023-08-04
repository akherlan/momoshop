#!/usr/bin/env python3

import pandas as pd

import os
import glob


def import_data(paths, header=None):
    data = [pd.read_csv(file, header=header) for file in paths]
    fields_len = len(data[0].columns)
    if not sum([len(p.columns) != fields_len for p in data]):
        data = pd.concat(data, ignore_index=True)
        data = data.drop_duplicates(ignore_index=True)
        if header == None:
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


def extract_dataset(data, name):
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
