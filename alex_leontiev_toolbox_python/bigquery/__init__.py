"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/bigquery/__init__.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-07-24T14:34:33.407557
    REVISION: ---

==============================================================================="""
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def table_exists(table_name, bq_client=None, entity="table"):
    assert entity in ["dataset", "table", "model"]
    if bq_client is None:
        bq_client = bigquery.Client()
    try:
        getattr(bq_client, f"get_{entity}")(table_name)
    except NotFound:
        return False
    return True


def query_bytes(sql, bq_client=None):
    if bq_client is None:
        bq_client = bigquery.Client()
    return bq_client.query(sql, job_cofig=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)).total_bytes_processed
