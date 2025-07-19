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
from alex_leontiev_toolbox_python.bigquery import query_bytes
from alex_leontiev_toolbox_python.bigquery.analysis import (
    _IS_SUPERKEY_KEYS_NO_NULL_TPL,
    _IS_SUPERKEY_IS_SUPERKEY_TPL,
)
import pandas as pd
import operator
import functools
import subprocess
import json
from jinja2 import Template


class _BigQuerySeries:
    def __init__(self, parent, column_name: str, is_column: bool = False):
        self._parent = parent
        self._column_name = column_name
        self._is_column: bool = is_column

    @property
    def is_column(self) -> bool:
        return self._is_column

    def nunique(self) -> int:
        df = self._parent._fetch_df(
            f"""
            with t as (
              select distinct {self._column_name} x
              from `{self._parent._table_name}`
            )
            select count(1) as n from t
        """
        )
        return df.iloc[0, 0]

    def unique(self) -> list:
        df = self._parent._fetch_df(
            f"""
              select distinct {self._column_name} x
              from `{self._parent._table_name}`
            order by 1
        """
        )
        return df["x"].to_list()

    @property
    def type(self) -> typing.Optional[str]:
        if self.is_column:
            (res,) = self._parent.schema.loc[
                self._parent.schema["name"].eq(self._column_name), "type"
            ]
            return res

    def describe(
        self,
        percentiles: list[typing.Union[str, float]] = ["0.25", "0.5", "0.75"],
        dry_run: bool = False,
    ) -> typing.Union[pd.Series, str]:
        sql = Template(
            """
                select cnt,
                  mean,
                  --std
                  mi,
                  {% for p in percentiles -%}
                    p{{loop.index0}},
                  {% endfor -%}
                  ma,
                
                from (
                  select
                    count(1) cnt,
                    {% if type=='STRING' %}
                    count(distinct {{cn}}) cd,
                    {% else -%}
                    avg({{cn}}) mean,
                    {% endif -%}
                    min({{cn}}) mi,
                    max({{cn}}) ma,
                  from `{{tn}}`
              )
                {% if (percentiles|length)>0 -%}
                cross join (
                select distinct
                  {% for p in percentiles -%}
                    percentile_cont({{cn}}, {{p}}) over () p{{loop.index0}},
                  {% endfor -%}
                from `{{tn}}`
               {% endif %}
              ) 
            """
        ).render(
            tn=self._parent._table_name,
            cn=self._column_name,
            type=self.type,
            percentiles=[] if self.type == "STRING" else percentiles,
        )
        if dry_run:
            return sql
        df = self._parent._fetch_df(sql)
        df.rename(
            columns={
                **{f"p{i}": f"{float(x)*100:.2f}%" for i, x in enumerate(percentiles)},
                "mi": "min",
                "ma": "max",
                "cnt": "count",
            },
            inplace=True,
        )
        (r,) = df.to_dict(orient="records")
        s = pd.Series(r)
        return s

    def value_counts(self) -> pd.Series:
        """
        FIXME: add limit on cardinality
        """
        df = self._parent._fetch_df(
            f"""
        select {self._column_name} x, count(1) cnt
        from `{self._parent._table_name}`
        group by 1
        order by 2 desc
        """
        )
        if self._is_column:
            df.rename(columns={"x": self._column_name}, inplace=True)
        return df.set_index(self._column_name if self._is_column else "x")["cnt"]


def _table_name_or_query(s: str) -> str:
    if " " in s:
        return "query"
    else:
        return "table_name"


_ANALYSIS_HOOKS = ["fetch_df", "fetch", "to_table", "is_superkey"]


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
        fetch: typing.Optional[typing.Callable] = None,
        to_table: typing.Optional[typing.Callable] = None,
        is_table_name: typing.Optional[bool] = None,
        description: typing.Optional[str] = None,
    ):
        index = tuple(sorted(set(index)))
        assert len(index) > 0, index

        self._logger = logging.getLogger(self.__class__.__name__)

        is_table_name = ((is_table_name is not None) and is_table_name) or (
            _table_name_or_query(table_name) == "table_name"
        )
        if is_table_name:
            self._table_name = table_name
        else:
            self._query = table_name
            self._table_name = to_table(table_name)
            table_name = self._table_name

        bq_client = bigquery.Client(**bq_client_kwargs)
        self._bq_client = bigquery.Client()

        self._index = index
        self._fetch_df = (
            (lambda sql: bq_client.query(sql).to_dataframe())
            if fetch_df is None
            else fetch_df
        )
        self._fetch = fetch
        self._to_table = to_table
        self._is_superkey = is_superkey
        self._size_limit = size_limit

        # actually, it does not speed up the process, for bq_client.get_table takes time
        # t = bq_client.get_table(table_name)
        self._t = None
        # self._t = t

        self._bytes_size = bytes_size

        if not is_skip:
            # schema = set(self.schema["name"])
            # assert set(index) <= set(schema), (index, schema)
            if size_limit is not None:
                # num_bytes = (
                #     query_bytes(self._query) if self.is_from_query else self.num_bytes
                # )
                # FIXME: more correct computation with to_table's templates
                num_bytes = query_bytes(
                    _IS_SUPERKEY_KEYS_NO_NULL_TPL.render(
                        table_name=table_name, candidate_superkey=index
                    )
                ) + query_bytes(
                    _IS_SUPERKEY_IS_SUPERKEY_TPL.render(
                        table_name=table_name, candidate_superkey=index, cnt_fn="cnt"
                    )
                )
                assert num_bytes <= size_limit, (num_bytes, size_limit)
            assert is_superkey(table_name, index), (table_name, index)

        self._head = None
        self._description = description

    @property
    def query(self) -> typing.Optional[str]:
        return self._query if self.is_from_query else None

    @property
    def is_from_query(self) -> bool:
        return hasattr(self, "_query")

    def items(self) -> list:
        return [(cn, self[cn]) for cn in self.schema["name"]]

    @property
    def sql(self):
        return "\n".join(
            [
                "",
                *(
                    []
                    if self._description is None
                    else [f"--{l}" for l in self._description.split("\n")]
                ),
                f"-- primary key: {self._index}",
                f"`{self.table_name}`",
            ]
        )

    @property
    def num_bytes(self):
        res = self._t.num_bytes if self._bytes_size is None else self._bytes_size
        # self._logger.warning(f"num_bytes: {res}")
        return res

    @functools.cached_property
    def schema(self) -> pd.DataFrame:
        return self.get_schema()

    def get_schema(self) -> pd.DataFrame:
        res = pd.DataFrame(map(operator.methodcaller("to_api_repr"), self.t.schema))
        res["is_primary"] = res["name"].isin(list(self.index))
        res.sort_values(
            by=["is_primary", "name"], ascending=[False, True], inplace=True
        )
        return res

    def _materialize(self, method="fetch"):
        if method == "fetch":
            assert self.num_bytes <= self._size_limit, (
                self.num_bytes,
                self._size_limit,
            )
            df = self._fetch(self._table_name)
        elif method == "fetch_df":
            assert self.num_bytes <= self._size_limit, (
                self.num_bytes,
                self._size_limit,
            )
            df = self._fetch_df(f"select * from {self._table_name}")
        else:
            df = method(self)
        self._df = df

    @property
    def df(self) -> pd.DataFrame:
        if (not hasattr(self, "_df")) or (self._df is None):
            self._materialize()
        df_res = self._df.copy()
        df_res.set_index(list(self.index), inplace=True)
        return df_res

    @property
    def head(self) -> pd.DataFrame:
        if self._head is None:
            self.get_head()
        return self._head

    def get_head(
        self, bq_exe: str = "bq", method="bq", limit: int = 100
    ) -> pd.DataFrame:
        if method == "bq":
            cmd = f'{bq_exe} head --format=json {self._table_name.replace(".", ":", 1)}'
            ec, out = subprocess.getstatusoutput(cmd)
            assert ec == 0, (cmd, ec, out)
            res = pd.DataFrame(json.loads(out))
        elif method == "bigquery":
            bq_client = bigquery.Client()
            res = bq_client.query(
                f"select * from {self.table_name} limit {limit}"
            ).to_dataframe()
        else:
            raise NotImplementedError(dict(method=method))

        self._head = res
        return res

    @functools.lru_cache
    def __getitem__(self, key: str):
        # assert
        return _BigQuerySeries(
            self,
            key,
            is_column=key.lower() in self.schema["name"].str.lower().to_list(),
        )

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
        return TableWithIndex(
            sql,
            self.index if result_key is None else result_key,
            is_skip=(not is_force_verify)
            and (join_key is None)
            and (result_key is None),
            description=f"join of {self} and `{right}` on {join_key} and {result_key}"
            ** {k: getattr(self, f"_{k}") for k in _ANALYSIS_HOOKS},
        )

    def dimensions(self) -> pd.DataFrame:
        raise NotImplementedError()

    def slice(
        self,
        is_force_verify: bool = False,
        is_exclude_sliced_cols: bool = True,
        **kwargs,
    ):
        assert len(kwargs) > 0
        kwargs = {k: to_list(v) for k, v in kwargs.items()}
        sliced_cols = [k for k, v in kwargs.items() if len(v) == 1]
        self._logger.warning(dict(kwargs=kwargs, sliced_cols=sliced_cols))
        return TableWithIndex(
            Template(
                """
                select *
                  {% if is_exclude_sliced_cols and (sliced_cols|length)>0 -%}
                    except ({{ sliced_cols|join(",") }})
                  {% endif -%}
                from {{ self_.sql }}
                where
                  {% for k,v in kwargs.items() -%}
                    (
                      {{k}}
                      {% if (v|length)==1 -%}
                        ={{ to_sql(v[0]) }}
                      {% else -%}
                        in (
                          {% for vv in v -%}
                            {{ to_sql(vv) }}
                            {{ "," if not loop.last }}
                          {% endfor -%}
                        )
                      {% endif -%}
                    )
                    {{ "and" if not loop.last }}
                  {% endfor -%}
            """
            ).render(
                self_=self,
                kwargs=kwargs,
                to_sql=to_sql,
                sliced_cols=sliced_cols,
                is_exclude_sliced_cols=is_exclude_sliced_cols,
            ),
            index=list(set(self.index) - set(sliced_cols)),
            **{k: getattr(self, f"_{k}") for k in _ANALYSIS_HOOKS},
            is_skip=(not is_force_verify),
        )

    @property
    def t(self):
        if self._t is None:
            self._t = self._bq_client.get_table(self._table_name)
        return self._t

    def where(self):
        raise NotImplementedError()

    def sample(
        self,
        n: typing.Optional[int] = None,
        frac: typing.Optional[float] = None,
        dry_run: bool = False,
        random_field_name: str = "r123456",
        is_force_verify: bool = False,
    ):
        if n is not None:
            sql = f"""
            with t as (select *, rand() {random_field_name} from `{self.table_name}`)
            select * except ({random_field_name})
            from t
            order by {random_field_name}
            limit {n}
            """
        elif frac is not None:
            sql = f"""
            with t as (select *, rand() {random_field_name} from `{self.table_name}`)
            select * except ({random_field_name})
            from t
            where {random_field_name}<{frac}
            """
        else:
            raise NotImplementedError()
        if dry_run:
            return sql
        else:
            return TableWithIndex(
                sql,
                self.index,
                is_skip=not is_force_verify,
                description=f"sample of {self} with n={n} and frac={frac}",
                **{k: getattr(self, f"_{k}") for k in _ANALYSIS_HOOKS},
            )


@functools.singledispatch
def to_sql(x) -> str:
    return str(x)


@to_sql.register
def _(x: str) -> str:
    return f'"""{x}"""'


@functools.singledispatch
def to_list(x) -> list:
    return [x]


@to_list.register
def _(x: list) -> list:
    return x


@functools.singledispatch
def to_table_name(x) -> str:
    raise NotImplementedError()


@to_table_name.register
def _(x: str) -> str:
    return f"`{x}`"


@to_table_name.register
def _(x: TableWithIndex) -> str:
    return x.sql
