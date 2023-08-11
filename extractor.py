#!/usr/bin/env python3

import pandas as pd
import json
from os import path


def import_data(paths, header=None, fmt="csv"):
    if fmt == "json":
        data = [pd.read_json(file) for file in paths]
        data = pd.concat(data, ignore_index=True)
        data = data.explode("variant_name", ignore_index=True).explode(
            "image", ignore_index=True
        )  # berrybenka
        return data
    data = [pd.read_csv(file, header=header) for file in paths]
    fields_len = len(data[0].columns)
    if not sum([len(p.columns) != fields_len for p in data]):
        data = pd.concat(data, ignore_index=True)
        data = data.drop_duplicates(ignore_index=True)
        if header is None:
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


def append_empty_columns(data, tbl="all", sch="schema.json"):
    with open(sch) as fin:
        schema = json.load(fin)
    tbls = list(schema.get("fields").keys())
    if tbl == "all":
        fields = []
        for tbl in tbls:
            fields.extend(schema.get("fields").get(tbl).get("name"))
        fields = list(set(fields))
    elif tbl in tbls:
        fields = schema.get("fields").get(tbl).get("name")
    else:
        raise ValueError("invalid tbl value")
    for key in fields:
        if key not in data.columns.to_list():
            data[key] = None
    return data


def extract_dataset(data, name: str, sch="schema.json"):
    with open(sch) as fin:
        schema = json.load(fin)
    if name not in list(schema.get("fields").keys()):
        raise ValueError("invalid name")
    fields_name = schema.get("fields").get(name).get("name")
    fields_unique = schema.get("fields").get(name).get("unique")
    fields_sort = schema.get("fields").get(name).get("sort")
    return (
        data[fields_name]
        .drop_duplicates(fields_unique)
        .sort_values(fields_sort, ignore_index=True)
    )


def append_dataset(dataset, master: str, dup=False, dup_name=None, fmt="parquet"):
    if fmt == "parquet":
        engine = "fastparquet"
        if not path.exists(master):
            print(f"{master} is not exists. Creating new.")
            dup_name = master
        else:
            master_dataset = pd.read_parquet(master, engine=engine)
            if dataset.columns.to_list() != master_dataset.columns.to_list():
                from_rows = dataset.columns.to_list()
                into_rows = master_dataset.columns.to_list()
                raise Exception(f"column is different, {from_rows} into {into_rows}")
            dataset = pd.concat([master_dataset, dataset]).drop_duplicates(
                ignore_index=True
            )
            if not dup:
                dup_name = master
            else:
                if dup_name is None:
                    dup_name = f"{master.replace('.parquet', '')}_copy.parquet"
        dataset.to_parquet(dup_name, engine=engine)
        print(f"Save data to {dup_name}")
    else:
        raise Exception("not parquet?")
