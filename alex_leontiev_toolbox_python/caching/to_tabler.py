"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/caching/to_tabler.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-07-24T13:57:32.170911
    REVISION: ---

FIXME:
    1. assume_sync
    2. smarter hashing
==============================================================================="""

from google.cloud import bigquery
import hashlib
import alex_leontiev_toolbox_python.utils


def _sql_to_hash(sql):
    return alex_leontiev_toolbox_python.utils.string_to_hash(sql, algo="md5")


class ToTabler:
    def __init__(self, prefix, bq_client=None, assume_sync=True):
        if bq_client is None:
            bq_client = bigquery.Client()
        assert not assume_sync
        self._assume_sync = assume_sync
        assert 2 <= len(prefix.split(".")) <= 3, prefix
        self._prefix = prefix
        self._client = bq_client

        _dataset = ".".join(prefix.split(".")[:2])
        assert alex_leontiev_toolbox_python.utils.table_exists(
            _dataset, bq_client=bq_client, entity="dataset"), f"ds \"{_dataset}\" does not exist"
        self._quota_used_bytes = 0

    @property
    def client(self):
        return self._client
    @property
    def quota_used_bytes(self):
        return self._quota_used_bytes

    def __call__(self, sql, preamble=None, dry_run):
        pass
