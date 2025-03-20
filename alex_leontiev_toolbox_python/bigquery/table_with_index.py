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
from alex_leontiev_toolbox_python.utils import format_bytes
import pandas as pd
import operator
import functools
import subprocess
import json


class _BigQuerySeries:
    def __init__(self, parent, column_name: str):
        self._parent = parent
        self._column_name = column_name

    @functools.cached_property
    def nunique(self) -> int:
        df = self._parent._fetch_df(
            f"""
        select count(distinct {self._column_name}) cnt
        from `{self._parent._table_name}`
        """
        )
        return df.iloc[0, 0]

    @functools.cached_property
    def value_counts(self) -> pd.Series:
        """
        FIXME: add limit on cardinality
        """
        df = self._parent._fetch_df(
            f"""
        select {self._column_name}, count(1) cnt
        from `{self._parent._table_name}`
        group by 1
        order by 2 desc
        """
        )
        return df["cnt"]


class TableWithIndex:
    def __init__(
        self,
        table_name: str,
        index: list[str],
        is_superkey: typing.Optional[typing.Callable] = None,
        is_skip: bool = False,
        bytes_size: typing.Optional[int] = None,
        bq_client_kwargs: dict = {},
        ## default=100G
        size_limit: typing.Optional[int] = 100 * 2 ** (3 * 10),
        fetch_df: typing.Optional[typing.Callable] = None,
    ):
        index = tuple(sorted(set(index)))
        assert len(index) > 0, index

        self._logger = logging.getLogger(self.__class__.__name__)
        self._table_name = table_name
        self._index = index
        self._fetch_df = fetch_df
        self._size_limit = size_limit

        bq_client = bigquery.Client(**bq_client_kwargs)
        t = bq_client.get_table(table_name)
        self._t = t
        schema = set(self.schema["name"])
        assert set(index) <= set(schema), (index, schema)

        self._bytes_size = bytes_size

        if not is_skip:
            if size_limit is not None:
                assert self.num_bytes <= size_limit, (self.num_bytes, size_limit)
            assert is_superkey(table_name, index), (table_name, index)

    @functools.cached_property
    def num_bytes(self):
        res = self._t.num_bytes if self._bytes_size is None else self._bytes_size
        # self._logger.warning(f"num_bytes: {res}")
        return res

    @functools.cached_property
    def schema(self) -> pd.DataFrame:
        return self.get_schema()

    def get_schema(self) -> pd.DataFrame:
        res = pd.DataFrame(map(operator.methodcaller("to_api_repr"), self._t.schema))
        res["is_primary"] = res["name"].isin(list(self.index))
        res.sort_values(
            by=["is_primary", "name"], ascending=[False, True], inplace=True
        )
        return res

    @functools.cached_property
    def df(self) -> pd.DataFrame:
        assert self.num_bytes <= self._size_limit, (self.num_bytes, self._size_limit)
        return self._fetch_df(f"select * from `{self._table_name}`")

    @functools.cached_property
    def head(self) -> pd.DataFrame:
        return self.get_head()

    def get_head(self, bq_exe: str = "bq"):
        cmd = f'{bq_exe} head --format=json {self._table_name.replace(".", ":", 1)}'
        ec, out = subprocess.getstatusoutput(cmd)
        assert ec == 0, (cmd, ec, out)
        return pd.DataFrame(json.loads(out))

    @functools.lru_cache
    def __getitem__(self, key: str):
        return _BigQuerySeries(self, key)

    def __str__(self):
        b = format_bytes(self.num_bytes)
        # self._logger.warning(f"__str__: {b}")
        return f"""{self.__class__.__name__}(table_name=`{self.table_name}`, index={self.index}, size={b})"""

    def __repr__(self):
        return str(self)

    def _repl_html_(self):
        return f"<tt>{str(self)}</tt>"

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def index(self) -> typing.Tuple[str]:
        return self._index

    def join(
        self,
        right,
        to_table: typing.Callable,
        join_sql: str = "join",
        is_force_verify: bool = False,
        join_key=None,
        result_key=None,
    ):
        # join_key = right.index
        join_key = right.index if join_key is None else join_key
        sql = f"""
        select
        from `{self.table_name}`
          {join_sql} `{right.table_name}`
          using ({','.join(join_key)})
        """
        self._logger.warning(sql)
        tn = to_table(sql)
        return TableWithIndex(
            tn,
            self.index if result_key is None else result_key,
            is_skip=(not is_force_verify)
            and (join_key is None)
            and (result_key is None),
        )

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
