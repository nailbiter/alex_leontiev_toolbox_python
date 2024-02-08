"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/caching/fetcher.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-07-24T13:57:38.736812
    REVISION: ---

==============================================================================="""

from sqlalchemy import create_engine, text
from google.cloud import bigquery
import sqlalchemy
import pandas as pd
import logging
import typing

_TABLE_NAME_TO_DB_NAME_CONNECTOR = "___"
_DASH_REPLACE = "__"

_POST_PROCESSORS = {"str": lambda df: df.map(str)}


class Fetcher:
    def __init__(
        self,
        sqlalchemy_db="sqlite+pysqlite:///:memory:",
        bq_client=None,
        db_prefix="t_",
        download_limit_gb=1,
        to_dataframe_kwargs={"progress_bar_type": "tqdm"},
        post_call_callbacks=[],
    ):
        if bq_client is None:
            bq_client = bigquery.Client()
        self._bq_client = bq_client
        self._sqlalchemy_engine = create_engine(
            sqlalchemy_db,
            #            echo=True,
            future=True,
        )
        self._db_prefix = db_prefix
        self._logger = logging.getLogger(self.__class__.__name__)
        self._download_limit_gb = download_limit_gb
        self._quota_used_bytes = 0
        self._to_dataframe_kwargs = to_dataframe_kwargs
        self._post_call_callbacks = post_call_callbacks

    def _db_table(self, table_name, post_process: typing.Optional[str]):
        db_table = self._db_prefix + _TABLE_NAME_TO_DB_NAME_CONNECTOR.join(
            table_name.split(".")
        )
        db_table = db_table.replace("-", _DASH_REPLACE)
        if post_process is not None:
            db_table = f"{db_table}_{post_process}"
        return db_table

    def __call__(
        self,
        table_name,
        is_return_debug_info=False,
        use_query_cache=True,
        to_dataframe_kwargs=None,
        post_process: typing.Optional[str] = None,
    ):
        if to_dataframe_kwargs is None:
            to_dataframe_kwargs = self._to_dataframe_kwargs
        db_table = self._db_table(table_name, post_process)
        d = {"table_name": table_name}
        if (
            sqlalchemy.inspect(self._sqlalchemy_engine).has_table(db_table)
            and use_query_cache
        ):
            d["is_executed"] = False
            self._logger.warning(f'fetching "{table_name}" from cache')
        else:
            d["is_executed"] = True
            num_bytes = self._bq_client.get_table(table_name).num_bytes
            d["num_bytes"] = num_bytes
            df = self._bq_client.query(f"select * from `{table_name}`").to_dataframe(
                **to_dataframe_kwargs
            )
            if post_process is not None:
                df = _POST_PROCESSORS[post_process](df)

            with self._sqlalchemy_engine.begin() as conn:
                df.to_sql(db_table, conn, if_exists="replace", index=False)
            self._quota_used_bytes += num_bytes

        with self._sqlalchemy_engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM {db_table};"), conn)

        for cb in self._post_call_callbacks:
            cb(d)

        return (df, d) if is_return_debug_info else df

    @property
    def quota_used_bytes(self):
        return self._quota_used_bytes
