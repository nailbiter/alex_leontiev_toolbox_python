"""===============================================================================

        FILE: tests/test_table_exists.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-07-24T14:37:39.603678
    REVISION: ---

==============================================================================="""

import alex_leontiev_toolbox_python.bigquery
from alex_leontiev_toolbox_python.bigquery import find_job_by_destination
from google.cloud import bigquery
import pandas as pd


def test_table_exists():
    bq_client = bigquery.Client()
    assert alex_leontiev_toolbox_python.bigquery.table_exists(
        "bigquery-public-data.usa_names.usa_1910_2013", bq_client=bq_client
    )
    assert alex_leontiev_toolbox_python.bigquery.table_exists(
        "bigquery-public-data.usa_names.usa_1910_2013"
    )
    assert alex_leontiev_toolbox_python.bigquery.table_exists(
        "bigquery-public-data.usa_names", bq_client=bq_client, entity="dataset"
    )
    assert not alex_leontiev_toolbox_python.bigquery.table_exists(
        "a.b.c", bq_client=bq_client
    )


def test_query_bytes():
    #    raise NotImplementedError()
    pass


def test_job_id_to_job():
    job = alex_leontiev_toolbox_python.bigquery.job_id_to_job(
        "38691128-02a7-4ec1-81f3-03dbca5b525d"
    )
    assert job.__class__.__name__ == "QueryJob"
    assert (
        job.query
        == "select * from `api-project-424250507607.b7a150d3.t_915d8cbae22f305fab72d94e9815e391`"
    )


def test_find_table_names_in_sql_source():
    assert alex_leontiev_toolbox_python.bigquery.find_table_names_in_sql_source(
        "select * from `api-project-424250507607.b7a150d3.t_915d8cbae22f305fab72d94e9815e391`"
    ) == {
        "api-project-424250507607.b7a150d3.t_915d8cbae22f305fab72d94e9815e391",
    }
    assert (
        alex_leontiev_toolbox_python.bigquery.find_table_names_in_sql_source(
            """
        --`api-project-424250507607.b7a150d3.t_uocuoueoeu`
        select * from `api-project-424250507607.b7a150d3.t_oeroygrccyo123123`
    """
        )
        == {
            "api-project-424250507607.b7a150d3.t_oeroygrccyo123123",
        }
    )


def test_list_tables():
    dates = [
        ts.to_pydatetime()
        for ts in pd.date_range(start="20160801", end="20170801").to_list()
    ]

    res = alex_leontiev_toolbox_python.bigquery.list_tables(
        "bigquery-public-data.google_analytics_sample.ga_sessions_"
    )
    assert len(res) == len(dates)
    assert res[0] == "20160801", res[0]

    res = alex_leontiev_toolbox_python.bigquery.list_tables(
        "bigquery-public-data.google_analytics_sample.ga_sessions_", is_parse_dates=True
    )
    assert len(res) == len(dates), res
    #    assert res[0] == datetime(2016, 8, 1)
    assert res == dates, res

    res = alex_leontiev_toolbox_python.bigquery.list_tables(
        "bigquery-public-data.google_analytics_sample.ga_sessions_",
        is_include_prefix=True,
    )
    assert len(res) == len(dates), res
    assert (
        res[0] == "bigquery-public-data.google_analytics_sample.ga_sessions_20160801"
    ), res[0]


def test_find_job_by_destination():
    bq_client = bigquery.Client()
    job = find_job_by_destination(
        f"{bq_client.project}.dataset.test_table",
        method="query_regex_search",
    )
    assert (
        job.query
        == f"""create or replace table `{bq_client.project}.dataset.test_table` as (select 1 x)"""
    )
