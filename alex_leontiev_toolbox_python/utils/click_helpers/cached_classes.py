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

import sqlite
import pandas as pd
import click
import re


def _fetch_uuid(uuid, uuid_cache_db=None):
    m = re.match(r"^(-?\d+)$", uuid)
    if m is not None:
        uuids_df = UuidCacher(uuid_cache_db).get_all()
        uuid = uuids_df.uuid.iloc[int(m.group(1))]
    return uuid


class UuidCacher:
    def __init__(self, cache_database_filename, db_name="uuid_cache"):
        self._cache_database_filename = cache_database_filename
        self._db_name = db_name
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_conn(self):
        conn = sqlite3.connect(self._cache_database_filename)
        return conn

    def add(self, uuid, name):
        df = pd.DataFrame(
            [{"uuid": uuid, "datetime": datetime.now().isoformat(), "name": name}]
        )
        conn = self._get_conn()
        df.to_sql(self._db_name, conn, if_exists="append", index=None)
        conn.close()
        self._logger.warning(f'add "{uuid}" to cache')

    def get_all(self):
        conn = self._get_conn()
        df = pd.read_sql(f"select * from {self._db_name}", conn)
        conn.close()

        df.datetime = df.datetime.apply(datetime.fromisoformat)
        df = df.sort_values(by="datetime")
        return df


class CachedString(click.ParamType):
    def __init__(self, cache_filename, table_filename="cached_string"):
        super().__init__()
        self._table_filename = table_filename
        self._cache_filename = cache_filename

    def convert(self, value, param, ctx):
        if (m := re.match(r"^-\d+$")) is not None:
            # TODO: fetch
            pass
        else:
            # TODO: save
            return value
        # self.fail(str(dict(value=value, )), param, ctx)
        pass
