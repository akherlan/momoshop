#!/usr/bin/env python3

import pandas as pd

import os
import glob

from extractor import import_data, extract_dataset


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
