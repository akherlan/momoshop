#!/usr/bin/env python3

import pandas as pd

import os
import glob

from extractor import import_data, extract_dataset


if __name__ == "__main__":
    paths = glob.glob("data/gaudi_[0-9]*.csv")
    data = import_data(paths, header="infer")

    if "date_published" not in data.columns.to_list():
        data["date_published"] = None

    products = extract_dataset(data, name="product")
    variants = extract_dataset(data, name="variant")
    prices = extract_dataset(data, name="price")

    products.to_csv("data/products_gaudi_concat.csv", index=False)
    variants.to_csv("data/variants_gaudi_concat.csv", index=False)
    prices.to_csv("data/prices_gaudi_concat.csv", index=False)

    for f in paths:
        os.remove(f)
        print(f"delete {f}")
