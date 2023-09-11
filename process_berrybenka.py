#!/usr/bin/env python3

import glob
import os

from extractor import import_data, extract_dataset, append_dataset, append_empty_columns


def main():
    # Load
    paths = glob.glob("data/berrybenka_[0-9]*.json")
    data = import_data(paths)
    data = append_empty_columns(data)

    # Transform
    print("Processing data...")
    products = extract_dataset(data, name="product")
    variants = extract_dataset(data, name="variant")
    prices = extract_dataset(data, name="price")
    # images = extract_dataset(data, name="image")

    append_dataset(products, "data/products.parquet")
    append_dataset(variants, "data/variants.parquet")
    append_dataset(prices, "data/prices.parquet")
    # append_dataset(images, "data/images.parquet", dup=True)  # error occure while saving

    for f in paths:
        os.remove(f)
        print(f"Delete {f}")


if __name__ == "__main__":
    main()
