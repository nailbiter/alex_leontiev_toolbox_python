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
import inspect
import types
from typing import cast
import logging
import re
import sqlparse


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

    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(
        types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)

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
        logger.warning(f"creating dataset {dataset}")

    return dataset


def job_id_to_job(job_id, bq_client=None, **kwargs):
    """
    FIXME: test
    """
    if bq_client is None:
        bq_client = bigquery.Client()
    return bq_client.get_job(job_id, **kwargs)


# https://cloud.google.com/bigquery/docs/tables
# https://cloud.google.com/bigquery/docs/datasets#dataset-naming
# https://cloud.google.com/resource-manager/docs/creating-managing-projects#before_you_begin
"""
FIXME (improve):
    1. 
"""
TABLE_NAME_REGEX = re.compile(
    r"""`([a-z0-9-]{5,29}[a-z0-9]\.[a-zA-Z0-9_]{1,1024}\.[\s\d\w]{1,1024})`""")


def find_table_names_in_sql_source(sql_source, bq_client=None, is_use_bq_client=False, table_name_regex=TABLE_NAME_REGEX):
    """
    FIXME:
        1. enable `is_use_bq_client`
    """
    if is_use_bq_client:
        raise NotImplementedError()

    if (bq_client is None) and is_use_bq_client:
        bq_client = bigquery.Client()

    sql_source = sqlparse.format(sql_source, strip_comments=True)

    return {tn for tn in table_name_regex.findall(sql_source)}
