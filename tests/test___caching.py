"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test___caching.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION:

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION:
     VERSION: ---
     CREATED: 2022-07-24T15:35:01.088019
    REVISION: ---

==============================================================================="""
import os
import alex_leontiev_toolbox_python.caching
import alex_leontiev_toolbox_python.caching.to_tabler
import alex_leontiev_toolbox_python.caching.fetcher
import alex_leontiev_toolbox_python.bigquery
from google.cloud import bigquery
import logging
import pandas as pd
import numpy as np
import string

IS_EXECUTE_BQ_TEST = int(os.environ.get("IS_EXECUTE_BQ_TEST", 0)) == 1


def test_to_table():
    if IS_EXECUTE_BQ_TEST:
        history = []
        location = "asia-northeast1"
        location = "US"
        bq_client = bigquery.Client(location=location)
        dataset_name = f"{os.environ['GCLOUD_PROJECT']}.b7a150d3"
        table_name = None
        try:
            alex_leontiev_toolbox_python.bigquery.create_dataset(
                dataset_name, location=location, exist_ok=True)
            to_table = alex_leontiev_toolbox_python.caching.to_tabler.ToTabler(
                f"{dataset_name}.t_", bq_client=bq_client, assume_sync=True)
            sql = """
                SELECT
                  gender
                  -- SUM(number) AS total
                FROM
                  `bigquery-public-data.usa_names.usa_1910_2013`
                -- GROUP BY
                --   name, gender
                -- ORDER BY
                --   total DESC
                LIMIT
                  10
            """
            table_name, debug = to_table(sql, is_return_debug_info=True)
            assert alex_leontiev_toolbox_python.bigquery.table_exists(
                table_name, bq_client=bq_client)
            assert debug["is_executed"]
            table_name, debug = to_table(sql, is_return_debug_info=True)
            assert not debug["is_executed"]

            logging.warning(to_table.quota_used_bytes)

            for kwargs in [{}, dict(sqlalchemy_db="sqlite+pysqlite:///.fetcher.db")]:
                fetch = alex_leontiev_toolbox_python.caching.fetcher.Fetcher(
                    bq_client=bq_client,
                    **kwargs,
                )
                df, debug = fetch(table_name, is_return_debug_info=True)
                assert len(df) == 10
                assert debug["is_executed"]
                df, debug = fetch(table_name, is_return_debug_info=True)
                assert not debug["is_executed"]
                logging.warning(fetch.quota_used_bytes)
        finally:
            if table_name is not None:
                bq_client.delete_table(table_name, not_found_ok=True)
            bq_client.delete_dataset(dataset_name, not_found_ok=True)


def test_upload():
    location = "US"
    bq_client = bigquery.Client(location=location)
    to_table = alex_leontiev_toolbox_python.caching.to_tabler.ToTabler(
        bq_client=bq_client)

    df = pd.DataFrame(np.random.randn(10, 10),
                      columns=list(string.ascii_lowercase)[:10])

    tables = set()

    try:
        tn, _d = to_table.upload_df(df, is_return_debug_info=True)
        tables.add(tn)
        assert _d["is_executed"]
        assert alex_leontiev_toolbox_python.bigquery.table_exists(
            tn, bq_client=bq_client)
        assert bq_client.get_table(tn).num_rows == 10

        tn, _d = to_table.upload_df(df, is_return_debug_info=True)
        tables.add(tn)
        assert not _d["is_executed"]

        bq_client.delete_table(tn)

        df = pd.DataFrame(np.random.randn(10, 10),
                          columns=list(string.ascii_uppercase)[:10])
        tn, _d = to_table.upload_df(
            df, is_return_debug_info=True, superkey=["A"])
        tables.add(tn)
        assert _d["is_executed"]
        assert _d["superkey"] == ["A"]
        assert alex_leontiev_toolbox_python.bigquery.table_exists(
            tn, bq_client=bq_client)
        assert bq_client.get_table(tn).num_rows == 10
    finally:
        for tn in tables:
            bq_client.delete_table(tn, not_found_ok=True)
