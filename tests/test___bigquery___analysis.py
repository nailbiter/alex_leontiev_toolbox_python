"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test___bigquery___analysis.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-07-28T23:38:45.745319
    REVISION: ---

==============================================================================="""
import os
import alex_leontiev_toolbox_python.caching
import alex_leontiev_toolbox_python.caching.to_tabler
import alex_leontiev_toolbox_python.caching.fetcher
import alex_leontiev_toolbox_python.bigquery
import alex_leontiev_toolbox_python.bigquery.analysis
from google.cloud import bigquery
import logging
import pandas as pd
import numpy as np
import string
import json
import functools


_TO_TABLER_KWARGS = {"wait_after_dataframe_upload_seconds": 5}


def test_field_coverage_stats():
    location = "US"
    bq_client = bigquery.Client(location=location)
    to_table = alex_leontiev_toolbox_python.caching.to_tabler.ToTabler(
        bq_client=bq_client, **_TO_TABLER_KWARGS)
    fetch = alex_leontiev_toolbox_python.caching.fetcher.Fetcher(
        bq_client=bq_client)
    field_coverage_stats = functools.partial(alex_leontiev_toolbox_python.bigquery.analysis.field_coverage_stats,
                                             to_table=to_table, fetch=fetch, is_return_debug_info=True)

    df1 = pd.DataFrame(np.random.randn(10, 10),
                       columns=list(string.ascii_lowercase)[:10])
    df2 = df1.copy()
    df1["i"] = ["x"]*5+["y"]*5
    df2["i"] = ["a"]*5+["b"]*5
    df3 = pd.concat([df1, df2])

    tables = set()

    try:
        tn1 = to_table.upload_df(df1)
        tn2 = to_table.upload_df(df2)
        tn3 = to_table.upload_df(df3)
        tables.update([tn1, tn2, tn3])

        res, d = field_coverage_stats(tn3, tn3, ["i"])
        assert d["sign"] == "==", (res, d)

        res, d = field_coverage_stats(tn1, tn2, ["i"])
        assert d["sign"] == "!=", (res, d)

        res, d = field_coverage_stats(tn1, tn3, ["i"])
        assert d["sign"] == "<=", (res, d)

        res, d = field_coverage_stats(tn3, tn1, ["i"])
        assert d["sign"] == ">=", (res, d)
    finally:
        for tn in tables:
            bq_client.delete_table(tn, not_found_ok=True)


def test_schema_to_df():
    bq_client = bigquery.Client()
    TN = "bigquery-public-data.usa_names.usa_1910_2013"
    res = alex_leontiev_toolbox_python.bigquery.analysis.schema_to_df(
        TN, is_table_name_input=True, is_return_comparable_object=True)
#    logging.warning(res)
#    with open("/tmp/e2b43790_f8c1_4ce3_859a_00b167d768de.txt", "w") as f:
#        f.write(res)
    _RES = """name     type      mode                        description
0   state   STRING  NULLABLE                 2-digit state code
1  gender   STRING  NULLABLE           Sex (M=male or F=female)
2    year  INTEGER  NULLABLE              4-digit year of birth
3    name   STRING  NULLABLE    Given name of a person at birth
4  number  INTEGER  NULLABLE  Number of occurrences of the name""".strip()
    assert res.strip() == _RES


def test_is_fields_dependent():
    location = "US"
    bq_client = bigquery.Client(location=location)
    to_table = alex_leontiev_toolbox_python.caching.to_tabler.ToTabler(
        bq_client=bq_client, **_TO_TABLER_KWARGS)
    fetch = alex_leontiev_toolbox_python.caching.fetcher.Fetcher(
        bq_client=bq_client)

    df = pd.DataFrame(np.random.randn(10, 10),
                      columns=list(string.ascii_lowercase)[:10])
    df["x"] = 1
    df["y"] = df["x"] + 1

    tables = set()

    try:
        tn = to_table.upload_df(df)
        tables.add(tn)
        assert alex_leontiev_toolbox_python.bigquery.table_exists(
            tn, bq_client=bq_client)
        res, d = alex_leontiev_toolbox_python.bigquery.analysis.is_superkey(
            tn, ["a"], to_table=to_table, fetch=fetch, is_return_debug_info=True)
        assert res, d
        res, d = alex_leontiev_toolbox_python.bigquery.analysis.is_fields_are_dependent(
            tn, ["x"], ["a"], to_table=to_table, fetch=fetch, is_return_debug_info=True)
        assert not res, json.dumps(d)
        res, d = alex_leontiev_toolbox_python.bigquery.analysis.is_fields_are_dependent(
            tn, ["x"], ["y"], to_table=to_table, fetch=fetch, is_return_debug_info=True)
        assert res, json.dumps(d)
    finally:
        for tn in tables:
            bq_client.delete_table(tn, not_found_ok=True)


def test_is_superkey():
    location = "US"
    bq_client = bigquery.Client(location=location)
    to_table = alex_leontiev_toolbox_python.caching.to_tabler.ToTabler(
        bq_client=bq_client, **_TO_TABLER_KWARGS)
    fetch = alex_leontiev_toolbox_python.caching.fetcher.Fetcher(
        bq_client=bq_client)
    is_superkey = functools.partial(alex_leontiev_toolbox_python.bigquery.analysis.is_superkey,
                                    to_table=to_table, fetch=fetch, is_return_debug_info=True)

    df = pd.DataFrame(np.random.randn(10, 10),
                      columns=list(string.ascii_lowercase)[:10])
    df["x"] = 1
    df["i"] = np.arange(len(df))

    tables = set()

    try:
        tn = to_table.upload_df(df)
        tables.add(tn)

        res, d = is_superkey(tn, ["i"])
        assert res, d

        res, d = is_superkey(tn, ["x"])
        assert not res, d
    finally:
        for tn in tables:
            bq_client.delete_table(tn, not_found_ok=True)


def test_is_tables_equal():
    location = "US"
    bq_client = bigquery.Client(location=location)
    to_table = alex_leontiev_toolbox_python.caching.to_tabler.ToTabler(
        bq_client=bq_client, **_TO_TABLER_KWARGS)
    fetch = alex_leontiev_toolbox_python.caching.fetcher.Fetcher(
        bq_client=bq_client)
    is_tables_equal = functools.partial(alex_leontiev_toolbox_python.bigquery.analysis.is_tables_equal,
                                        to_table=to_table, fetch=fetch, is_return_debug_info=True)

    df = pd.DataFrame(np.random.randn(10, 10),
                      columns=list(string.ascii_lowercase)[:10])
    df["i"] = np.arange(len(df))
    df_shuffled = df.sample(frac=1.0)
    assert len(df) == len(df_shuffled)
    df2 = df.copy()
    df2["a"] = 42.0
    assert len(df) == len(df2)

    tables = set()

    d = {}
    try:
        tn = to_table.upload_df(df)
        tn_shuffled = to_table.upload_df(df_shuffled)
        tn2 = to_table.upload_df(df2)
        tables.add(tn)
        tables.update([tn, tn2, tn_shuffled])

        res, d = is_tables_equal(tn, tn_shuffled, ["i"])
        assert res, d

        res, d = is_tables_equal(tn, tn2, ["i"])
        assert not res, d

#        assert False, d
    except Exception:
        with open("/tmp/E33F2262-AE5B-4489-8B57-F9876D57FD1B.log.json", "w") as f:
            json.dump({k: v for k, v in d.items()
                      if k not in "diff_sql_f".split()}, f)
        raise
    finally:
        for tn in tables:
            bq_client.delete_table(tn, not_found_ok=True)
