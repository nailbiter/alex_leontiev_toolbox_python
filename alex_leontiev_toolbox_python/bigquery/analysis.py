"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/bigquery/analysis.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-07-28T20:26:41.031833
    REVISION: ---

==============================================================================="""
import pandas as pd
from jinja2 import Template
from google.cloud import bigquery
from alex_leontiev_toolbox_python.utils import melt_single_record_df
from alex_leontiev_toolbox_python.caching.fetcher import Fetcher
from alex_leontiev_toolbox_python.caching.to_tabler import ToTabler
import inspect
import types
from typing import cast
import typing
import logging
import functools


_DEFAULT_TABLE_ALIASES = ("tn1", "tn2")


class IndexedTable:
    def __init__(table_name: str, superkey: list[str], fetch_to_table: typing.Tuple):
        self._table_name = table_name
        self._superkey = superkey
        self._fetch_to_table = fetch_to_table

        fetch, to_table = fetch_to_table
        assert is_superkey(table_name, superkey, fetch=fetch, to_table=to_table)

    @property
    def table_name(self):
        return self._table_name

    @property
    def superkey(self):
        return self._superkey

    def to_dict(self) -> dict:
        return dict(table_name=self.table_name, superkey=self.superkey)


def schema_to_df(
    table_or_table_name: typing.Union[str, bigquery.Table],
    bq_client: typing.Optional[bigquery.Client] = None,
    is_return_comparable_object: bool = False,
    is_table_name_input: bool = False,
) -> typing.Union[str, pd.DataFrame]:
    if (bq_client is None) and is_table_name_input:
        bq_client = bigquery.Client()
    table = (
        bq_client.get_table(table_or_table_name)
        if is_table_name_input
        else table_or_table_name
    )
    df = pd.DataFrame([sf.to_api_repr() for sf in table.schema])
    return df.to_string() if is_return_comparable_object else df


def _original_field_name(fields):
    pass


def is_superkey(
    table_name: str,
    candidate_superkey: list[str],
    fetch=None,
    to_table=None,
    is_return_debug_info=False,
    is_normalize_keys=True,
    count_field_name="cnt",
    fetch_lines=0,
):
    """
    0<=?"indexeness"<=1
    "indexeness"==1 <==> is_superkey

    fetch_lines==-1 <==> fetch all

    FIXME:
        1. make `count_field_name` automatic (_original_field_name)
        2. only subset of fields
        3. fetch_counterexamples (num_lines)
        4(done). "indexeness"
    """
    assert -1 <= fetch_lines

    if fetch is None:
        fetch = Fetcher()
    if to_table is None:
        to_table = ToTabler()
    if is_normalize_keys:
        candidate_superkey = sorted(set(candidate_superkey))
    d = {}
    d["rendered_sql"] = Template(
        """
        select {{candidate_superkey|join(",")}}, count(1) {{cnt_fn}},
        from `{{table_name}}`
        group by {{candidate_superkey|join(",")}}
        having {{cnt_fn}}>1
    """
    ).render(
        {
            "candidate_superkey": candidate_superkey,
            "table_name": table_name,
            "cnt_fn": count_field_name,
        }
    )
    d["diff_tn"] = to_table(d["rendered_sql"])
    if fetch_lines == -1:
        d["diff_df"] = fetch(d["diff_tn"])
    elif fetch_lines > 0:
        d["diff_df"] = fetch(
            to_table(
                f"""
        select *
        from `{d["diff_tn"]}`
        order by {",".join(candidate_superkey)}
        limit {fetch_lines}
        """
            )
        )

    num_rows = to_table.client.get_table(d["diff_tn"]).num_rows
    d["indexeness"] = 1 / (num_rows + 1)
    res = d["indexeness"] == 1
    return (res, d) if is_return_debug_info else res


def field_coverage_stats(
    table_name_1,
    table_name_2,
    fields,
    fetch=None,
    to_table=None,
    is_return_debug_info=False,
    is_normalize_keys=True,
    aliases=_DEFAULT_TABLE_ALIASES,
    weight: typing.Optional[typing.Callable] = None,
):
    if fetch is None:
        fetch = Fetcher()
    if to_table is None:
        to_table = ToTabler()
    if is_normalize_keys:
        fields = sorted(set(fields))
    d = {}
    ## FIXME: compress duplicated code (t1 vs t2)
    ## in Template below
    ## info for-loop
    d["rendered_sql"] = Template(
        """
    with t1 as (
        select {{fields|join(",")}},
          {% if weight is none %}
            1
          {% else %} 
            {{ weight(0) }}
          {% endif %} 
          {{ aliases[0] }},
        from `{{table_name_1}}`
        group by {{fields|join(",")}}
    )
    , t2 as (
        select {{fields|join(",")}},
          {% if weight is none %}
            1
          {% else %} 
            {{ weight(1) }}
          {% endif %} 
          {{ aliases[1] }},
        from `{{table_name_2}}`
        group by {{fields|join(",")}}
    )
    , t as (
        select {{fields|join(",")}},
            ifnull({{aliases[0]}}, 0) {{aliases[0]}},
            ifnull({{aliases[1]}}, 0) {{aliases[1]}},
        from t1 full outer join t2 using ({{fields|join(",")}})
    )
    select {{aliases|join(",")}},
        {% if weight is none %}
        count(1) cnt
        {% else %}
        sum({{ aliases[0] }}) sum_0,
        sum({{ aliases[1] }}) sum_1,
        {% endif %}
    from t
    group by {{aliases[0]}}, {{aliases[1]}}
    """
    ).render(
        {
            "table_name_1": table_name_1,
            "table_name_2": table_name_2,
            "aliases": aliases,
            "fields": fields,
            "weight": weight,
        }
    )
    d["tn"] = to_table(d["rendered_sql"])
    df = fetch(d["tn"])

    df[list(aliases)] = df[list(aliases)] == 1
    df["flag"] = list(zip(df.pop(aliases[0]), df.pop(aliases[1])))
    if df.flag.nunique() == 1:
        d["sign"] = "=="
    elif df.flag.apply(lambda t: t[0]).nunique() == 1:
        d["sign"] = ">="
    elif df.flag.apply(lambda t: t[1]).nunique() == 1:
        d["sign"] = "<="
    else:
        d["sign"] = "!="
    df["label"] = df.pop("flag").apply(
        {
            (True, True): "∩".join(aliases),
            (False, True): "-".join(list(aliases)[::-1]),
            (True, False): "-".join(aliases),
        }.get
    )
    df.set_index("label", inplace=True)
    df.sort_index(inplace=True)

    return (df, d) if is_return_debug_info else df


def _diff_sql_f(fn, aliases=None):
    return (
        Template(
            """
    ({{aliases[0]}}_{{fn}}!={{aliases[1]}}_{{fn}}) or (({{aliases[0]}}_{{fn}} is null) and ({{aliases[1]}}_{{fn}} is not null)) or (({{aliases[0]}}_{{fn}} is not null) and ({{aliases[1]}}_{{fn}} is null))
    """
        )
        .render({"fn": fn, "aliases": aliases})
        .strip()
    )


def is_tables_equal(
    table_name_1,
    table_name_2,
    superkey,
    fetch=None,
    to_table=None,
    join="full outer join",
    is_return_debug_info=False,
    is_normalize_keys=True,
    aliases=_DEFAULT_TABLE_ALIASES,
    fetch_diff_how_many_rows=0,
    is_check_same_rows=True,
    is_check_same_schema=True,
    compare_fields=None,
    numerical_fields=[],
    numerical_tolerance=0,
    is_melt_diff=False,
):
    """
    * `numerical_fields` must be included into `compare_fields` (we check this)

    TODO:
        1. is_melt_diff
    FIXME:
        1. replace `message` with enum?
    """
    # FIXME: unimplemented
    if is_melt_diff:
        raise NotImplementedError
    assert len(numerical_fields) == 0

    # default args
    if fetch is None:
        fetch = Fetcher()
    if to_table is None:
        to_table = ToTabler()

    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

    # normalization
    if is_normalize_keys:
        superkey = sorted(set(superkey))
        numerical_fields = sorted(set(numerical_fields))
        if compare_fields is not None:
            compare_fields = sorted(set(compare_fields))

    d = {}
    d["diff_sql_f"] = functools.partial(_diff_sql_f, aliases=aliases)

    # sanity
    assert table_name_1 != table_name_2, "same names"
    assert is_superkey(
        table_name_1, superkey, to_table=to_table, fetch=fetch
    ), f"{superkey} is not superkey to {aliases[0]}"
    assert is_superkey(
        table_name_2, superkey, to_table=to_table, fetch=fetch
    ), f"{superkey} is not superkey to {aliases[1]}"

    res = False
    _schema_to_df = functools.partial(
        schema_to_df, bq_client=to_table.client, is_table_name_input=True
    )
    for _ in range(1):
        # compare schema
        d["schema_1"] = _schema_to_df(table_name_1, is_return_comparable_object=True)
        d["schema_2"] = _schema_to_df(table_name_2, is_return_comparable_object=True)
        if is_check_same_schema and (d["schema_1"] != d["schema_2"]):
            d["message"] = "different schema"
            break

        # compare num rows
        d["num_rows_1"] = to_table.client.get_table(table_name_1).num_rows
        d["num_rows_2"] = to_table.client.get_table(table_name_2).num_rows
        if is_check_same_rows and (d["num_rows_1"] != d["num_rows_2"]):
            d["message"] = "different number of fields"
            break

        # figure out fields to compare
        if compare_fields is None:
            compare_fields = set(_schema_to_df(table_name_1).name) & set(
                _schema_to_df(table_name_2).name
            )
            compare_fields -= set(superkey)
            compare_fields = sorted(compare_fields)
        assert set(numerical_fields) <= set(compare_fields)
        d["compare_fields"] = compare_fields

        d["rendered_sql"] = Template(
            """
        with joined as (
            select {{superkey|join(",")}},
                {%for f in compare_fields%}
                {{aliases[0]}}.{{f}} {{aliases[0]}}_{{f}},
                {{aliases[1]}}.{{f}} {{aliases[1]}}_{{f}},
                {%endfor%}
            from `{{table_name_1}}` {{aliases[0]}} {{join}} `{{table_name_2}}` {{aliases[1]}} using ({{superkey|join(",")}})
        )
        select *
        from joined
        where {%for f in compare_fields%}
        ({{diff_sql_f(f)}}) {{"or" if not loop.last}}
        {%endfor%}
        """
        ).render(
            {
                "join": join,
                "compare_fields": compare_fields,
                "superkey": superkey,
                "diff_sql_f": d["diff_sql_f"],
                "aliases": aliases,
                "table_name_1": table_name_1,
                "table_name_2": table_name_2,
            }
        )
        d["diff_tn"] = to_table(d["rendered_sql"])

        res = to_table.client.get_table(d["diff_tn"]).num_rows == 0
    return (res, d) if is_return_debug_info else res


def is_fields_are_dependent(
    table_name,
    fields_x,
    fields_y,
    fetch=None,
    to_table=None,
    is_return_debug_info=False,
    is_normalize_keys=True,
    is_generate_reduction=False,
):
    if fetch is None:
        fetch = Fetcher()
    if to_table is None:
        to_table = ToTabler()
    if is_normalize_keys:
        fields_x = sorted(set(fields_x))
        fields_y = sorted(set(fields_y))
    assert set(fields_x) & set(fields_y) == set()
    d = {}
    tn = to_table(
        Template(
            """
    select distinct {{fields|join(",")}}, 
    from `{{table_name}}`
    """
        ).render(
            {
                "fields": fields_x + fields_y,
                "table_name": table_name,
            }
        )
    )
    res, d["is_superkey_debug_info"] = is_superkey(
        tn, fields_x, to_table=to_table, fetch=fetch, is_return_debug_info=True
    )
    return (res, d) if is_return_debug_info else res


_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS = {
    "cntd": (lambda fn: f"count (distinct {fn})"),
    "cntn": (lambda fn: f"countif ({fn} is null)"),
}


def bq_describe(
    table_name,
    fields,
    operations=list(_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS),
    presets=_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS,
    is_normalize_keys=True,
    to_table=None,
    fetch=None,
):
    pass


DEFAULT_IMPUTATION_VALUES_PER_TYPE = {
    ## https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types
    "ARRAY": "[]",
    "BIGNUMERIC": -1,
    "BOOL": "FALSE",
    # "BYTES": ,
    "DATE": "datetime(9999,12,24)",
    "DATETIME": "datetime(9999,12,24)",
    "FLOAT64": -1,
    "FLOAT": -1,
    # "GEOGRAPHY": ,
    "INT64": -1,
    "INTEGER": -1,
    # "INTERVAL": ,
    # "JSON": ,
    "NUMERIC": -1,
    "STRING": "***IFNULL***",
    # "STRUCT": ,
    # "TIME": ,
    # "TIMESTAMP": ,
}


def is_nonull(
    table_name: str,
    fields: list[str],
    fetch=None,
    to_table=None,
    is_return_debug_info: bool = False,
    cnt_name: str = "cnt",
    is_generate_imputed_table: bool = False,
    imputation_values_per_type: dict = {},
) -> typing.Union[bool, typing.Tuple[bool, dict]]:
    """
    FIXME: enable `is_generate_imputed_table`
    """
    assert cnt_name not in fields, (cnt_name, fields)

    imputation_values_per_type = {
        **DEFAULT_IMPUTATION_VALUES_PER_TYPE,
        **imputation_values_per_type,
    }

    jinja_env = dict(
        table_name=table_name,
        cnt_name=cnt_name,
        fields=fields,
    )
    d = {
        "sql": Template(
            """
                select
                    {%for k in fields%}
                    countif({{k}} is null)/count(1)*100 {{k}},
                    {%endfor%}
                    count(1) {{cnt_name}},
                from `{{table_name}}`
                """
        ).render(jinja_env)
    }
    df = fetch(to_table(d["sql"]))
    df = melt_single_record_df(
        df, fields_to_preserve=[cnt_name], column_names=("column_name", "null_perc")
    )
    d["df"] = df

    if is_generate_imputed_table:
        _schema_to_df = functools.partial(
            schema_to_df, bq_client=to_table.client, is_table_name_input=True
        )
        schema_df = _schema_to_df(table_name)
        imputation_values = {
            field_name: imputation_values_per_type[type_]
            for field_name, type_ in schema_df[["name", "type"]].values
            if field_name in fields
        }
        d["imputation_values"] = imputation_values
        d["imputed_table_name"] = to_table(
            Template(
                """
                select
                {% for cn in column_names %}
                  {% if cn in fields %}
                    ifnull({{ cn }}, {{ imputation_values[cn] }}) {{ cn }},
                  {% else %}
                    {{ cn }},
                  {% endif %}
                {% endfor %}
                from `{{ table_name }}`
                """
            ).render({**jinja_env, **d, "column_names": schema_df["name"].to_list()})
        )
        # raise NotImplementedError()

    res = (df["null_perc"] == 0).all()
    return (res, d) if is_return_debug_info else res
