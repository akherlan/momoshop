#!/usr/bin/env python3

import glob
import os

from extractor import import_data, extract_dataset, append_dataset, append_empty_columns

from urllib.parse import urlparse


def main():
    # Load
    paths = glob.glob("data/berrybenka_[0-9]*.json")
    data = import_data(paths, fmt="json")
    
    # Transform
    data["brand"] = "Berrybenka"
    data['source'] = data["link"].apply(lambda x: urlparse(x).netloc)
    data['slug'] = data["link"].apply(lambda x: urlparse(x).path)
    data['product_id'] = data["link"].apply(lambda x: list(filter(lambda y: y.isdigit(), x.split("/")))[0])

    data = append_empty_columns(data)

    products = extract_dataset(data, name="product")
    variants = extract_dataset(data, name="variant")
    images = extract_dataset(data, name="image")
    prices = extract_dataset(data, name="price")

    append_dataset(products, "data/products.parquet")
    append_dataset(variants, "data/variants.parquet")
    append_dataset(images, "data/images.parquet")
    append_dataset(prices, "data/prices.parquet")

    for f in paths:
        os.remove(f)
        print(f"Delete {f}")


if __name__ == "__main__":
    main()
