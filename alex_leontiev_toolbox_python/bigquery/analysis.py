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


def schema_to_df(table_or_table_name, bq_client=None, is_return_comparable_object=False):
    table = table_or_table_name if bq_client is None else bq_client.get_table(
        table_or_table_name)
    return pd.DataFrame([sf.to_api_repr() for sf in table.schema]) if


def is_superkey(table_name, candidate_superkey, fetcher=None, to_table=None, is_return_debug_info=False, is_normalize_keys=True):
    pass


def field_coverage_stats(table_name_1, table_name_2, fields, fetcher=None, to_table=None, is_return_debug_info=False, is_normalize_keys=True, aliases=("tn1", "tn2")):
    pass


def is_tables_equal(table_name_1, table_name_2, superkey, fetcher=None, to_table=None, join="join", is_return_debug_info=False, is_normalize_keys=True, , aliases=("tn1", "tn2"), fetch_diff_how_many_rows=0, is_check_same_rows=True, is_check_same_schema=True, compare_fields=None, numerical_fields=[], numerical_tolerance=0):
    """
    * `numerical_fields` must be included into `compare_fields` (we check this)
    """
    pass


def is_fields_dependent(table_name_1, table_name_2, fields_x,fields_y, fetcher=None, to_table=None, is_return_debug_info=False, is_normalize_keys=True, is_generate_reduction=False):
    pass

_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS = {
    "cntd":(lambda fn:f"count (distinct {fn})"),
    "cntn":(lambda fn:f"countif ({fn} is null)"),
}
def bq_describe(table_name,fields,operations=list(_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS),presets=_DEFAULT_BQ_DESCRIBE_OPERATIONS_PRESETS,is_normalize_keys=True,to_table=None,fetcher=None):
    pass
