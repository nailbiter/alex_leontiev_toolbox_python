"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/click_helpers/cached_classes.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-10-23T22:04:20.510926
    REVISION: ---

==============================================================================="""

import sqlite3
import pandas as pd
import click
import re
import logging
from datetime import datetime, timedelta


def _fetch_uuid(uuid, uuid_cache_db=None):
    m = re.match(r"^(-?\d+)$", uuid)
    if m is not None:
        uuids_df = UuidCacher(uuid_cache_db).get_all()
        uuid = uuids_df.uuid.iloc[int(m.group(1))]
    return uuid


class UuidCacher:
    def __init__(self, cache_database_filename: str, db_name: str = "uuid_cache"):
        self._cache_database_filename = cache_database_filename
        self._db_name = db_name
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_conn(self):
        conn = sqlite3.connect(self._cache_database_filename)
        return conn

    def add(self, name: str) -> None:
        df = pd.DataFrame([{"name": name, "datetime": datetime.now().isoformat()}])
        conn = self._get_conn()
        df.to_sql(self._db_name, conn, if_exists="append", index=None)
        conn.close()
        self._logger.warning(f'add "{name}" to cache')

    def get_all(self) -> pd.DataFrame:
        conn = self._get_conn()
        df = pd.read_sql(f"select * from {self._db_name}", conn)
        conn.close()

        df["datetime"] = df["datetime"].apply(datetime.fromisoformat)
        df.sort_values(by="datetime", inplace=True, ignore_index=True)
        return df


def fetch_or_pass(value: str, uuid_cacher: UuidCacher) -> str:
    if (m := re.match(r"^(-\d+)$", value)) is not None:
        df = uuid_cacher.get_all()
        return df.iloc[int(m.group(1))]["name"]
    else:
        uuid_cacher.add(value)
        return value


class CachedString(click.ParamType):
    def __init__(self, cache_db_filename: str, table_filename: str = "cached_string"):
        super().__init__()
        self._table_filename = table_filename
        self._cache_db_filename = cache_db_filename
        self._uuid_cacher = UuidCacher(cache_db_filename, table_filename)

    def convert(self, value, param, ctx):
        return fetch_or_pass(value, self._uuid_cacher)
        # self.fail(str(dict(value=value, )), param, ctx)
