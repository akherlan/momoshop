#!/usr/bin/env python3

import pandas as pd
import json
from os import path
import re


def detect_fmt(fname: str):
    pattern = r"^(.+)\.(.+)$"
    res = re.search(pattern, fname)
    if res is not None:
        filename, fmt = res.groups()
        return fmt
    else:
        return res


def import_data_single(path, **pdargs):
    fmt = detect_fmt(path)
    if fmt == "json":
        data = pd.read_json(path)
        listcol = data.apply(lambda x: any(isinstance(i, list) for i in x)).to_dict()
        listcol = list(filter(lambda x: x[1], listcol.items()))
        listcol = list(map(lambda x: x[0], listcol))
        for col in listcol:
            data = data.explode(col, ignore_index=True)
    elif fmt == "csv":
        data = pd.read_csv(path, **pdargs)
    else:
        raise ValueError("prefer format in csv or json")
    return data


def import_data(paths, **pdargs):
    data = [import_data_single(path, **pdargs) for path in paths]
    fields_len = len(data[0].columns)
    if not sum([len(p.columns) != fields_len for p in data]):
        data = pd.concat(data).drop_duplicates(ignore_index=True)
        return data
    else:
        raise Exception("different columns length")


def append_empty_columns(data, tbl: str = "all", sch: str = "schema.json"):
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


def extract_dataset(data, name: str, sch: str = "schema.json"):
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


def append_dataset(dataset, master: str, dup: bool = False, dup_name=None):
    if detect_fmt(master) == "parquet":
        engine = "fastparquet"
        if not path.exists(master):
            print(f"{master} is not exists. Creating new.")
            dup_name = master
        else:
            master_dataset = pd.read_parquet(master, engine=engine)
            fields = dataset.columns.to_list()
            if fields != master_dataset.columns.to_list():
                into_fields = master_dataset.columns.to_list()
                raise Exception(f"column is different\n\t{fields} into {into_fields}")
            # make sure the dtype is not different
            # for col in fields:
            #     dataset[col] = dataset[col].astype(master_dataset[col].dtypes.name)
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
        raise Exception("currently only support parquet")
