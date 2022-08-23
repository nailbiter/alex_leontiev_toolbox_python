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
#from alex_leontiev_toolbox_python.utils import is_pandas_superkey
from alex_leontiev_toolbox_python.caching.fetcher import Fetcher
from alex_leontiev_toolbox_python.caching.to_tabler import ToTabler
import inspect
import types
from typing import cast
import logging
import functools


def schema_to_df(table_or_table_name, bq_client=None, is_return_comparable_object=False, is_table_name_input=False):
    if bq_client is None:
        bq_client = bigquery.Client()
    table = bq_client.get_table(
        table_or_table_name) if is_table_name_input else table_or_table_name
    df = pd.DataFrame([sf.to_api_repr() for sf in table.schema])
    return df.to_string() if is_return_comparable_object else df


def _original_field_name(fields):
    pass


def is_superkey(table_name, candidate_superkey, fetch=None, to_table=None, is_return_debug_info=False, is_normalize_keys=True, count_field_name="cnt", fetch_lines=0):
    """
    0<=?"indexeness"<=1
    "indexeness"==1 <==> is_superkey

    FIXME: 
        1. make `count_field_name` automatic (_original_field_name)
        2. only subset of fields
        3. fetch_counterexamples (num_lines)
        4(done). "indexeness"
    """
    if fetch is None:
        fetch = Fetcher()
    if to_table is None:
        to_table = ToTabler()
    if is_normalize_keys:
        candidate_superkey = sorted(set(candidate_superkey))
    d = {}
    d["rendered_sql"] = Template("""
        select {{candidate_superkey|join(",")}}, count(1) {{cnt_fn}},
        from `{{table_name}}`
        group by {{candidate_superkey|join(",")}}
        having {{cnt_fn}}>1
    """).render({
        "candidate_superkey": candidate_superkey,
        "table_name": table_name,
        "cnt_fn": count_field_name,
    })
    d["diff_tn"] = to_table(d["rendered_sql"])
    if fetch_lines > 0:
        d["diff_df"] = fetch(d["diff_tn"])

    num_rows = to_table.client.get_table(d["diff_tn"]).num_rows
    d["indexeness"] = 1/(num_rows+1)
    res = (d["indexeness"] == 1)
    return (res, d) if is_return_debug_info else res


def field_coverage_stats(table_name_1, table_name_2, fields, fetch=None, to_table=None, is_return_debug_info=False, is_normalize_keys=True, aliases=("tn1", "tn2")):
    pass


_DEFAULT_TABLE_ALIASES = ("tn1", "tn2")


def _diff_sql_f(fn, aliases=None):
    return Template("""
    ({{aliases[0]}}_{{fn}}!={{aliases[1]}}_{{fn}}) or (({{aliases[0]}}_{{fn}} is null) and ({{aliases[1]}}_{{fn}} is not null)) or (({{aliases[0]}}_{{fn}} is not null) and ({{aliases[1]}}_{{fn}} is null))
    """).render({"fn": fn, "aliases": aliases}).strip()


def is_tables_equal(table_name_1, table_name_2, superkey, fetch=None, to_table=None, join="full outer join", is_return_debug_info=False, is_normalize_keys=True, aliases=_DEFAULT_TABLE_ALIASES, fetch_diff_how_many_rows=0, is_check_same_rows=True, is_check_same_schema=True, compare_fields=None, numerical_fields=[], numerical_tolerance=0, is_melt_diff=False):
    """
    * `numerical_fields` must be included into `compare_fields` (we check this)

    TODO:
        1. is_melt_diff
    FIXME:
        1. replace `message` with enum?
    """
    #FIXME: unimplemented
    if is_melt_diff:
        raise NotImplementedError
    assert len(numerical_fields) == 0

    # default args
    if fetch is None:
        fetch = Fetcher()
    if to_table is None:
        to_table = ToTabler()

    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(
        types.FrameType, inspect.currentframe()).f_code.co_name
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
    assert is_superkey(table_name_1, superkey, to_table=to_table,
                       fetch=fetch), f"{superkey} is not superkey to {aliases[0]}"
    assert is_superkey(table_name_2, superkey, to_table=to_table,
                       fetch=fetch), f"{superkey} is not superkey to {aliases[1]}"

    res = False
    _schema_to_df = functools.partial(
        schema_to_df, bq_client=to_table.client, is_table_name_input=True)
    for _ in range(1):
        # compare schema
        d["schema_1"] = _schema_to_df(
            table_name_1, is_return_comparable_object=True)
        d["schema_2"] = _schema_to_df(
            table_name_2, is_return_comparable_object=True)
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
                _schema_to_df(table_name_2).name)
            compare_fields -= set(superkey)
            compare_fields = sorted(compare_fields)
        assert set(numerical_fields) <= set(compare_fields)
        d["compare_fields"] = compare_fields

        d["rendered_sql"] = Template("""
        with joined as (
            select {{superkey|join(",")}},
                {%for f in compare_fields%}
                {{aliases[0]}}.{{f}} {{aliases[0]}}_{{f}},
                {{aliases[1]}}.{{f}} {{aliases[1]}}_{{f}},
                {%endfor%}
            from `{{table_name_1}}` {{aliases[0]}} {{join}} `{{table_name_1}}` {{aliases[1]}} using ({{superkey|join(",")}})
        )
        select *
        from joined
        where {%for f in compare_fields%}
        ({{diff_sql_f(f)}}) {{"or" if not loop.last}}
        {%endfor%}
        """).render({
            "join": join,
            "compare_fields": compare_fields,
            "superkey": superkey,
            "diff_sql_f": d["diff_sql_f"],
            "aliases": aliases,
            "table_name_1": table_name_1,
            "table_name_2": table_name_2,
        })
        d["diff_tn"] = to_table(d["rendered_sql"])

        res = (to_table.client.get_table(d["diff_tn"]).num_rows == 0)
    return (res, d) if is_return_debug_info else res


def is_fields_are_dependent(table_name, fields_x, fields_y, fetch=None, to_table=None, is_return_debug_info=False, is_normalize_keys=True, is_generate_reduction=False):
    if fetch is None:
        fetch = Fetcher()
    if to_table is None:
        to_table = ToTabler()
    if is_normalize_keys:
        fields_x = sorted(set(fields_x))
        fields_y = sorted(set(fields_y))
    assert set(fields_x) & set(fields_y) == set()
    d = {}
    tn = to_table(Template("""
    select distinct {{fields|join(",")}}, 
    from `{{table_name}}`
    """).render({
        "fields": fields_x+fields_y,
        "table_name": table_name,
    }))
    res, d["is_superkey_debug_info"] = is_superkey(tn, fields_x, to_table=to_table,
                                                   fetch=fetch, is_return_debug_info=True)
    return (res, d) if is_return_debug_info else res


_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS = {
    "cntd": (lambda fn: f"count (distinct {fn})"),
    "cntn": (lambda fn: f"countif ({fn} is null)"),
}


def bq_describe(table_name, fields, operations=list(_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS), presets=_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS, is_normalize_keys=True, to_table=None, fetch=None):
    pass
