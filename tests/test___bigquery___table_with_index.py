"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test___bigquery___table_with_index.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-12-25T21:26:29.928133
    REVISION: ---

==============================================================================="""
import logging
import unittest
from alex_leontiev_toolbox_python.bigquery.table_with_index import (
    TableWithIndex,
    to_sql,
    to_list,
    to_table_name,
)
from alex_leontiev_toolbox_python.bigquery.table_with_index import TableWithIndex

# from alex_leontiev_toolbox_python.caching.fetcher import Fetcher
from alex_leontiev_toolbox_python.caching.to_tabler import ToTabler
from alex_leontiev_toolbox_python.bigquery.analysis import is_superkey
import functools
import typing
import itertools
from google.cloud import bigquery
import numpy as np
import pandas as pd
import uuid
import os
from google.cloud import bigquery
from datetime import datetime, timedelta


class PseudoFetcher:
    def __init__(self, bq_client: bigquery.Client, **kwargs):
        self._bq_client = bq_client

    def __call__(self, tn: str) -> pd.DataFrame:
        return self._bq_client.query(f"select * from `{tn}`").to_dataframe()


def create_dataset_with_table_expiration(
    project_id, dataset_id, location="US", expiration_days=1
):
    """
    Creates a BigQuery dataset with a specified name and sets the default table expiration.

    Args:
        project_id (str): Your Google Cloud Project ID.
        dataset_id (str): The ID of the dataset to create.
        location (str, optional): The location of the dataset. Defaults to "US".
        expiration_days (int, optional): The number of days after which tables should expire. Defaults to 1.
    """

    client = bigquery.Client(project=project_id)

    # Construct a full Dataset object to send to the API.
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = location

    try:
        dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
        print(f"Created dataset {client.project}.{dataset.dataset_id}")

        # Set the default table expiration for the dataset
        default_table_expiration = (
            timedelta(days=expiration_days).total_seconds() * 1000
        )  # Convert to milliseconds
        dataset.default_table_expiration_ms = default_table_expiration
        dataset = client.update_dataset(dataset, ["default_table_expiration_ms"])
        print(
            f"Set default table expiration to {expiration_days} days for dataset {client.project}.{dataset.dataset_id}"
        )

    except Exception as e:
        print(f"An error occurred: {e}")
    return dataset


def test_basic():
    assert to_sql(3) == "3"
    assert to_sql(True) == "True"
    assert to_sql("x") == '"""x"""'

    assert to_list([1, 2, 3]) == [1, 2, 3]
    assert to_list(1) == [1]
    assert to_list(["x"]) == ["x"]


def test_materialize():
    try:
        # Example with other location and expiration time.
        # create_dataset_with_table_expiration(project_id, dataset_id, location='europe-west2', expiration_days=2)

        bq_client = bigquery.Client()
        tmp_dataset_name = f"""{os.getenv("GCLOUD_PROJECT")}.tmp_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"""
        tmp_dataset_name
        tmp_dataset = create_dataset_with_table_expiration(
            *tmp_dataset_name.split("."), location="US"
        )
        tmp_dataset

        to_table = ToTabler(
            prefix=f"{tmp_dataset_name}.t_",
            bq_client=bq_client,
            assume_sync=True,
            is_loud=True,
        )
        # fetch = Fetcher(bq_client=bq_client, is_loud=True)
        fetch = PseudoFetcher(bq_client=bq_client, is_loud=True)

        df1 = get_random_frame()
        tn = to_table.upload_df(df1, ["i1", "i2", "i3"])
        twi1 = TableWithIndex(
            tn,
            ["i1", "i2", "i3"],
            to_table=to_table,
            fetch=fetch,
            is_superkey=functools.partial(is_superkey, fetch=fetch, to_table=to_table),
            fetch_df=lambda sql: fetch(to_table(sql)),
        )
        df = twi1.df
        assert df.index.names == ["i1", "i2", "i3"]
        assert len(df) == len(df1)
        assert set(map(tuple, df.index)) == set(
            map(tuple, df1[["i1", "i2", "i3"]].values)
        )
    finally:
        bq_client.delete_dataset(tmp_dataset_name, delete_contents=True)


def test_slice():
    tmp_dataset_name, bq_client = [None] * 2
    try:
        # Example with other location and expiration time.
        # create_dataset_with_table_expiration(project_id, dataset_id, location='europe-west2', expiration_days=2)
        bq_client, tmp_dataset_name, to_table, fetch = setup()

        df1 = get_random_frame()
        tn = to_table.upload_df(df1, ["i1", "i2", "i3"])
        twi1 = TableWithIndex(
            tn,
            ["i1", "i2", "i3"],
            to_table=to_table,
            fetch=fetch,
            is_superkey=functools.partial(is_superkey, fetch=fetch, to_table=to_table),
            fetch_df=lambda sql: fetch(to_table(sql)),
        )

        assert set(twi1.index) == {"i1", "i2", "i3"}
        assert twi1.t.num_rows == 10**3

        twi2 = twi1.slice(i1=5, i2=[3, 4], is_force_verify=True)
        assert set(twi2.index) == {"i2", "i3"}
        assert twi2.t.num_rows == 2 * 10**1

        twi3 = twi1.sample(n=10, is_force_verify=True)
        assert set(twi3.index) == {"i1", "i2", "i3"}
        assert twi3.t.num_rows == 10

    finally:
        if tmp_dataset_name is not None and bq_client is not None:
            bq_client.delete_dataset(tmp_dataset_name, delete_contents=True)


def setup() -> (bigquery.Client, str, typing.Callable, typing.Callable):
    bq_client = bigquery.Client()
    tmp_dataset_name = f"""{os.getenv("GCLOUD_PROJECT")}.tmp_dataset_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"""
    tmp_dataset_name
    tmp_dataset = create_dataset_with_table_expiration(
        *tmp_dataset_name.split("."), location="US"
    )
    tmp_dataset

    to_table = ToTabler(
        prefix=f"{tmp_dataset_name}.t_",
        bq_client=bq_client,
        assume_sync=True,
        is_loud=True,
    )
    # fetch = Fetcher(bq_client=bq_client, is_loud=True)
    fetch = PseudoFetcher(bq_client=bq_client, is_loud=True)
    return bq_client, tmp_dataset_name, to_table, fetch


def test_to_table_name():
    assert to_table_name("a.b.c") == "`a.b.c`"
    assert (
        to_table_name(
            TableWithIndex(
                "bigquery-public-data.samples.gsod",
                ["station_number", "wban_number"],
                is_skip=True,
            )
        )
        == ""
    )


def test_join():
    tmp_dataset_name, bq_client = [None] * 2
    try:
        bq_client, tmp_dataset_name, to_table, fetch = setup()

        assert False, "todo"
        # df1 = get_random_frame()
        # tn = to_table.upload_df(df1, ["i1", "i2", "i3"])
        # twi1 = TableWithIndex(
        #     tn,
        #     ["i1", "i2", "i3"],
        #     to_table=to_table,
        #     fetch=fetch,
        #     is_superkey=functools.partial(is_superkey, fetch=fetch, to_table=to_table),
        #     fetch_df=lambda sql: fetch(to_table(sql)),
        # )

        # assert set(twi1.index) == {"i1", "i2", "i3"}
        # assert twi1.t.num_rows == 10**3

        # twi2 = twi1.slice(i1=5, i2=[3, 4], is_force_verify=True)
        # assert set(twi2.index) == {"i2", "i3"}
        # assert twi2.t.num_rows == 2 * 10**1

        # twi3 = twi1.sample(n=10, is_force_verify=True)
        # assert set(twi3.index) == {"i1", "i2", "i3"}
        # assert twi3.t.num_rows == 10

    finally:
        if tmp_dataset_name is not None and bq_client is not None:
            bq_client.delete_dataset(tmp_dataset_name, delete_contents=True)


def get_random_frame(random_seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(random_seed)
    df = pd.DataFrame(
        itertools.product(np.arange(10), np.arange(10), np.arange(10)),
        columns=["i1", "i2", "i3"],
    )
    df["a"] = df["i1"] + df["i2"] + df["i3"]
    return df


# class TestBigqueryTableWithIndex(unittest.TestCase):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._logger = logging.getLogger(self.__class__.__name__)

#     def test_something(self):
#         self.assertTrue(1 == 1)
#         self.assertEqual(1, 1)
#         self.assertNotEqual(1, 2)
