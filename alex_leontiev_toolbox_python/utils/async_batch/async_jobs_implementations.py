"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/async_batch/async_jobs_implementations.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-10-28T18:59:34.932629
    REVISION: ---

==============================================================================="""

from alex_leontiev_toolbox_python.utils.async_batch import AsyncJob
from datetime import datetime, timedelta
from google.cloud import bigquery


class BigQueryQueryJob(AsyncJob):
    def __init__(self, query: str, bq_client=None, query_kwargs: dict = {}):
        self._query = query
        self._bq_client = bigquery.Client() if bq_client is None else bq_client
        self._query_kwargs = query_kwargs

    def start(self):
        self._job = self._bq_client.query(self._query, **self._query_kwargs)

    def is_running(self):
        """
        https://cloud.google.com/python/docs/reference/bigquery/latest/index.html#google.cloud.bigquery.job.QueryJob.destination
        """
        return self._job.running

    def get_result(self):
        return self._job


class SleepJob(AsyncJob):
    def __init__(self, sleep_seconds: int):
        self._sleep_seconds = sleep_seconds
        self._started = None

    def start(self):
        self._started = datetime.now()

    def is_running(self):
        return ((datetime.now() - self._started).total_seconds()) <= self._sleep_seconds

    def get_result(self):
        return self._sleep_seconds
