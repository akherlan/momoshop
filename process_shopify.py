#!/usr/bin/env python3

import glob
import os

from extractor import import_data, extract_dataset, append_dataset, append_empty_columns


def main():
    # Load
    product_paths = glob.glob("data/products_[!a-z]*.csv")
    offer_paths = glob.glob("data/offers_[!a-z]*.csv")

    data_products = import_data(product_paths)
    # data_products.columns = [
    #     "product_id",
    #     "sku",
    #     "name",
    #     "brand",
    #     "category",
    #     "variant_id",
    #     "variant_name",
    #     "date_published",
    #     "description",
    #     "slug",
    # ]
    data_pricing = import_data(offer_paths, header=None)
    data_pricing.columns = [
        "product_id",
        "variant_id",
        "sku",
        "price",
        "is_instock",
        "date_acquisition",
        "source",
    ]
    data_products = append_empty_columns(data_products)
    data_pricing = append_empty_columns(data_pricing)

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
