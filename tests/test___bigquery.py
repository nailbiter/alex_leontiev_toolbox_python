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
from google.cloud import bigquery


def test_table_exists():
    bq_client = bigquery.Client()
    assert alex_leontiev_toolbox_python.bigquery.table_exists(
        "bigquery-public-data.usa_names.usa_1910_2013", bq_client=bq_client)
    assert alex_leontiev_toolbox_python.bigquery.table_exists(
        "bigquery-public-data.usa_names.usa_1910_2013")
    assert alex_leontiev_toolbox_python.bigquery.table_exists(
        "bigquery-public-data.usa_names", bq_client=bq_client, entity="dataset")
    assert not alex_leontiev_toolbox_python.bigquery.table_exists(
        "a.b.c", bq_client=bq_client)


def test_query_bytes():
    #    raise NotImplementedError()
    pass


def test_job_id_to_job():
    job = alex_leontiev_toolbox_python.bigquery.job_id_to_job(
        "38691128-02a7-4ec1-81f3-03dbca5b525d")
    assert job.__class__.__name__ == "QueryJob"
    assert job.query == "select * from `api-project-424250507607.b7a150d3.t_915d8cbae22f305fab72d94e9815e391`"


def test_find_table_names_in_sql_source():
    assert alex_leontiev_toolbox_python.bigquery.find_table_names_in_sql_source(
        "select * from `api-project-424250507607.b7a150d3.t_915d8cbae22f305fab72d94e9815e391`") == {"api-project-424250507607.b7a150d3.t_915d8cbae22f305fab72d94e9815e391", }
    assert alex_leontiev_toolbox_python.bigquery.find_table_names_in_sql_source(
        """
        --`api-project-424250507607.b7a150d3.t_uocuoueoeu`
        select * from `api-project-424250507607.b7a150d3.t_oeroygrccyo123123`
    """) == {"api-project-424250507607.b7a150d3.t_oeroygrccyo123123", }
