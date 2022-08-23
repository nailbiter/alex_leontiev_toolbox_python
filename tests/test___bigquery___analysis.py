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


def test_is_fields_dependent():
    location = "US"
    bq_client = bigquery.Client(location=location)
    to_table = alex_leontiev_toolbox_python.caching.to_tabler.ToTabler(
        bq_client=bq_client)
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
        bq_client=bq_client)
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
        bq_client=bq_client)
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
    df2["a"] = 42
    assert len(df) == len(df2)

    tables = set()

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
    finally:
        for tn in tables:
            bq_client.delete_table(tn, not_found_ok=True)
