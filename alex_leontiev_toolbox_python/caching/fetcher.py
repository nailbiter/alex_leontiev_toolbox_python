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
from alex_leontiev_toolbox_python.utils.logging_helpers import get_configured_logger
from alex_leontiev_toolbox_python.bigquery import schema_to_df

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
        is_loud: bool = True,
        schema_converters: dict = {},
        log_creation_kwargs: dict = {},
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
        self._is_loud = is_loud
        self._logger = get_configured_logger(
            self.__class__.__name__, level="INFO", **log_creation_kwargs
        )
        self._download_limit_gb = download_limit_gb
        self._quota_used_bytes = 0
        self._to_dataframe_kwargs = to_dataframe_kwargs
        self._post_call_callbacks = post_call_callbacks
        self._schema_converters = schema_converters

    def _db_table(self, table_name, post_process: typing.Optional[str]):
        db_table = self._db_prefix + _TABLE_NAME_TO_DB_NAME_CONNECTOR.join(
            table_name.split(".")
        )
        db_table = db_table.replace("-", _DASH_REPLACE)
        if post_process is not None:
            db_table = f"{db_table}_{post_process}"
        return db_table

    def _warning(self, *args, **kwargs):
        return self._log(*args, method="warning", **kwargs)

    def _debug(self, *args, **kwargs):
        return self._log(*args, method="debug", **kwargs)

    def _info(self, *args, **kwargs):
        return self._log(*args, method="info", **kwargs)

    def _error(self, *args, **kwargs):
        return self._log(*args, method="error", **kwargs)

    def _log(self, *args, method="warning", **kwargs):
        if self._is_loud:
            if method == "warning":
                return self._logger.warning(*args, **kwargs)
            elif method == "error":
                return self._logger.error(*args, **kwargs)

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
            self._debug(f'fetching "{table_name}" from cache')
        else:
            d["is_executed"] = True
            num_bytes = self._bq_client.get_table(table_name).num_bytes
            d["num_bytes"] = num_bytes
            df = self._bq_client.query(f"select * from `{table_name}`").to_dataframe(
                **to_dataframe_kwargs
            )
            sdf = schema_to_df(table_name, is_table_name_input=True)
            self._logger.debug(sdf)
            for k, f in self._schema_converters.items():
                if sdf["type"].eq(k).any():
                    self._logger.debug(f"applying schema convertor `{k}`")
                    for cn in sdf.loc[sdf["type"].eq(k), "name"]:
                        self._logger.debug(
                            f"applying schema convertor `{k}` for column {cn}"
                        )
                        df[cn] = df[cn].apply(f)
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
