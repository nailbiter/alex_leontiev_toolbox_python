"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/bigquery/table_with_index.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-12-20T20:58:47.503127
    REVISION: ---

==============================================================================="""
import typing
import logging
from google.cloud import bigquery
import pandas as pd
import operator
import functools


class TableWithIndex:
    def __init__(
        self,
        table_name: str,
        index: list[str],
        is_superkey: typing.Optional[typing.Callable] = None,
        _is_skip: bool = False,
    ):
        index = tuple(index)
        assert len(index) > 0, index
        if not _is_skip:
            assert is_superkey(table_name, index), (table_name, index)
        self._table_name = table_name
        self._index = index
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def index(self) -> typing.Tuple[str]:
        return self._index

    def join(self, right, to_table: typing.Callable, join_sql: str = "join"):
        sql = f"""
        select
        from `{self.table_name}`
          {join_sql} `{right.table_name}`
          using ({','.join(right.index)})
        """
        self._logger.warning(sql)
        tn = to_table(sql)
        return TableWithIndex(tn, self.index, _is_skip=True)

    @functools.cached_property
    def schema_df(self) -> pd.DataFrame:
        bq_client = bigquery.Client()
        res = pd.DataFrame(
            map(
                operator.methodcaller("to_api_repr"),
                bq_client.get_table(self.table_name).schema,
            )
        )
        res["is_key"] = res["name"].isin(self.index)
        return res
