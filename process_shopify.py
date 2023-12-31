#!/usr/bin/env python3

import glob
import os

from extractor import import_data, extract_dataset, append_dataset, append_empty_columns


def main():
    # Load
    product_paths = glob.glob("data/products_[!a-z]*.csv")
    offer_paths = glob.glob("data/offers_[!a-z]*.csv")

    data_products = import_data(product_paths)
    data_pricing = import_data(offer_paths)
    data_products = append_empty_columns(data_products)

    # Transform
    print("Processing data...")
    products = extract_dataset(data_products, name="product")
    variants = extract_dataset(data_products, name="variant")
    prices = extract_dataset(data_pricing, name="price")

    append_dataset(products, "data/products.parquet")
    append_dataset(variants, "data/variants.parquet")
    append_dataset(prices, "data/prices.parquet")

    for f in product_paths + offer_paths:
        os.remove(f)
        print(f"Delete {f}")


if __name__ == "__main__":
    main()
