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
    return bq_client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)).total_bytes_processed


def create_dataset(dataset_name, bq_client=None, exist_ok=False, location=None):
    """
    based on https://cloud.google.com/bigquery/docs/datasets#create-dataset
    """
    if bq_client is None:
        bq_client = bigquery.Client()

    if table_exists(dataset_name, bq_client=bq_client, entity="dataset"):
        assert exist_ok, f"dataset \"{dataset_name}\" exists"
        dataset = bq_client.get_dataset(dataset_name)
        if location is not None:
            assert dataset.location == location, (dataset.location, location)
    else:
        dataset = bigquery.Dataset(dataset_name)
        if location is not None:
            dataset.location = location
        dataset = bq_client.create_dataset(dataset, timeout=30)

    return dataset
