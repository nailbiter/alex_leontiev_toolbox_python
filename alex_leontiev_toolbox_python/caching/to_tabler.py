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


class ToTabler:
    def __init__(self, prefix, bq_client=None, assume_sync=True):
        if bq_client is None:
            bq_client = bigquery.Client()
        assert not assume_sync
        self._assume_sync = assume_sync
        assert 2 <= len(prefix.split(".")) <= 3, prefix
        self._prefix = prefix
        self._client = bq_client

    @property
    def client(self):
        return self._client

    def __call__(self, sql, preamble=None):
        pass
