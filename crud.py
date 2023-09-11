#!/usr/bin/env python3

import logging

# logging setups need to define before importing supabase
# https://github.com/supabase-community/supabase-py/issues/303
LOG_FORMAT_STRING = "%(asctime)s %(name)s [%(levelname)s] %(message)s"
logger = logging.getLogger()
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.basicConfig(format=LOG_FORMAT_STRING, level=logging.INFO, datefmt="%Y-%m-%d %H:%M:%S")

import os
import json
import ast

from supabase import create_client, Client

from postgrest.exceptions import APIError
from httpx import ReadTimeout, ConnectTimeout, ConnectError
from gotrue.errors import AuthApiError

from dotenv import load_dotenv


class DbClient:
    def __init__(self, url:str, key:str):
        self.client = create_client(url, key)

    def create_user(self, email:str, pswd:str) -> bool:
        res = self.client.auth.sign_up({"email": email, "password": pswd})
        if res:
            logger.info("User created!")
            return True
        else:
            return False

    def login_user(self, email:str, pswd:str) -> bool:
        try:
            res = self.client.auth.sign_in_with_password({"email": email, "password": pswd})
            if res:
                print(res)
                print(type(res))
                logger.info("Logged in!")
                return True
            else:
                return False
        except AuthApiError as e:
            logger.error(str(e))

    def __insert_row(self, table_name:str, pk:str, item:dict, conflict:str="update") -> tuple:
        try:
            res = self.client.table(table_name).insert([item]).execute()
            met = "insert"
        except APIError as e:
            err = ast.literal_eval(str(e))
            if err.get("code") == "23505": # duplication / conflict
                met = conflict
                if conflict == "update":
                    # res = self.client.table(table_name).update(item).eq(pk, str(item[pk])).execute()
                    res = self.client.table(table_name).upsert(item).execute()
                    assert len(res.data) > 0
                else:
                    res = self.client.table(table_name).select("*").eq(pk, str(item[pk])).execute()
                    assert len(res.data) > 0
        return (json.loads(res.model_dump_json()), met)

    def add_product(self, data:list, conflict:str = "update") -> None:
        if conflict in ("update", "abort"):
            for n, item in enumerate(data):
                try:
                    res, met = self.__insert_row(table_name="product", pk="product_id", item=item, conflict=conflict)
                    for i in res.get("data"):
                        logger.info(f'{met.upper()} {i.get("product_id")} {i.get("brand")} {i.get("name")}')
                        # logger.info(i)
                except (ReadTimeout, ConnectTimeout, ConnectError) as e:
                    logger.error(f"{str(e)} at data {n}")
                    break
            logger.info("Add product done!")


def main() -> None:
    with open("data/products.json") as f:
        product = json.load(f)
        logger.info(f"LEN: {len(product)}")
        product = product[26995:]
    load_dotenv()
    supabase = DbClient(
        url=os.environ.get("SUPABASE_URL"),
        key=os.environ.get("SUPABASE_KEY")
    )
    # RLS issue: https://github.com/supabase-community/supabase-py/issues/185
    # auth = supabase.login_user(
    #     email=os.environ.get("SUPABASE_URL"),
    #     pswd=os.environ.get("SUPABASE_PSSWD")
    # )
    auth = True
    if auth:
        supabase.add_product(data=product)


if __name__ == '__main__':
    main() 
