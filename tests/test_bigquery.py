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
    pass
