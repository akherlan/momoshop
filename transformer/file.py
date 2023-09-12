import pandas as pd
import re
import os
import glob
import json


class FileHandler:
    def __init__(self, name:str = None):
        self.name = name

    def __file_format(self, fname: str) -> str:
        pattern = r"^(.+)\.(.+)$"
        res = re.search(pattern, fname)
        if res is not None:
            filename, fmt = res.groups()
            return fmt
        else:
            return str()

    def __import_data_single(self, path:str, **pdargs):
        fmt = self.__file_format(path)
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

    def load(self, path:str, pattern:str, **pdargs) -> pd.DataFrame:
        filepattern = os.path.join(path, pattern)
        paths = glob.glob(filepattern)
        data = [self.__import_data_single(p, **pdargs) for p in paths]
        for p in paths:
            print("reading {}".format(p))
        fields_len = len(data[0].columns)
        if not sum([len(d.columns) != fields_len for d in data]):
            data = pd.concat(data).drop_duplicates(ignore_index=True)
            return data
        else:
            raise Exception("different columns length")

    def export(self, data:pd.DataFrame, file:str, **pdargs) -> None:
        if "orient" not in {**pdargs}.keys():
            orient = "records"
        else:
            orient = {**pdargs}.pop("orient")
        data.to_json(file, orient=orient, **pdargs)

    def reshape(self, datacls, obj:str, file:str) -> None:
        with open(obj) as fin:
            data = json.load(fin)
            data = [datacls(**x) for x in data]
            with open(file, "w") as fout:
                data = [x.model_dump() for x in data]
                json.dump(data, fout)

    def create_path(self, path:str, table:str="master", sep:str="_") -> str:
        fmt = "json"
        head = "data" if self.name is None else self.name
        filename = sep.join([head, table])
        return os.path.join(path, "{}.{}".format(filename, fmt))
