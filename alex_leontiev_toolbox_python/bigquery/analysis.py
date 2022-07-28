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


def schema_to_df(table_or_table_name, bq_client=None, is_return_comparable_object=False):
    table = table_or_table_name if bq_client is None else bq_client.get_table(
        table_or_table_name)
    df = pd.DataFrame([sf.to_api_repr() for sf in table.schema])
    return df.to_string() if is_return_comparable_object else df


def is_superkey(table_name, candidate_superkey, fetch=None, to_table=None, is_return_debug_info=False, is_normalize_keys=True, count_field_name="cnt"):
    if fetch is None:
        fetch = fetch()
    if to_table is None:
        to_table = to_table()
    if is_normalize_keys:
        candidate_superkey = sorted(set(candidate_superkey))
    d = {}
    df = fetch(to_table(Template("""
    with t as (
        select {{candidate_superkey|join(",")}}, count(1) {{cnt_fn}},
        from `{{table_name}}`
        group by {{candidate_superkey|join(",")}}
    )
    select max({{cnt_fn}}) {{cnt_fn}}
    from t
    """).render({
        "candidate_superkey": candidate_superkey,
        "table_name": table_name,
        "cnt_fn": count_field_name,
    })))
    res = df[count_field_name].iloc[0] == 1
    return (res, d) if is_return_debug_info else res


def field_coverage_stats(table_name_1, table_name_2, fields, fetch=None, to_table=None, is_return_debug_info=False, is_normalize_keys=True, aliases=("tn1", "tn2")):
    pass


def is_tables_equal(table_name_1, table_name_2, superkey, fetch=None, to_table=None, join="join", is_return_debug_info=False, is_normalize_keys=True, aliases=("tn1", "tn2"), fetch_diff_how_many_rows=0, is_check_same_rows=True, is_check_same_schema=True, compare_fields=None, numerical_fields=[], numerical_tolerance=0):
    """
    * `numerical_fields` must be included into `compare_fields` (we check this)
    """
    pass


def is_fields_are_dependent(table_name, fields_x, fields_y, fetch=None, to_table=None, is_return_debug_info=False, is_normalize_keys=True, is_generate_reduction=False):
    if fetch is None:
        fetch = fetch()
    if to_table is None:
        to_table = to_table()
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
    res, d = is_superkey(tn, fields_x, to_table=to_table,
                         fetch=fetch, is_return_debug_info=True)
    return (res, d) if is_return_debug_info else res


_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS = {
    "cntd": (lambda fn: f"count (distinct {fn})"),
    "cntn": (lambda fn: f"countif ({fn} is null)"),
}


def bq_describe(table_name, fields, operations=list(_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS), presets=_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS, is_normalize_keys=True, to_table=None, fetch=None):
    pass
