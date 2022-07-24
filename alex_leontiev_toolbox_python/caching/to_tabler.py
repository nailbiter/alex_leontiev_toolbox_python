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
import alex_leontiev_toolbox_python.bigquery
import alex_leontiev_toolbox_python.caching
from jinja2 import Template
import json
from datetime import datetime, timedelta
import logging


def _sql_to_hash(sql):
    return alex_leontiev_toolbox_python.utils.string_to_hash(sql, algo="md5")


# https://cloud.google.com/bigquery/docs/tables#table_naming
_TABLE_NAME_LENGTH_MAX = 1024


class ToTabler:
    def __init__(self, prefix, bq_client=None, assume_sync=False, post_call_callbacks=[]):
        if bq_client is None:
            bq_client = bigquery.Client()
        assert not assume_sync
        self._assume_sync = assume_sync
        assert 2 <= len(prefix.split(".")) <= 3, prefix
        if len(prefix.split(".")) == 2:
            prefix += "."
        self._prefix = prefix
        self._client = bq_client

        _dataset = ".".join(prefix.split(".")[:2])
        assert alex_leontiev_toolbox_python.bigquery.table_exists(
            _dataset, bq_client=bq_client, entity="dataset"), f"ds \"{_dataset}\" does not exist"
        self._quota_used_bytes = 0
        self._post_call_callbacks = post_call_callbacks
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def client(self):
        return self._client

    @property
    def quota_used_bytes(self):
        return self._quota_used_bytes

    def __call__(self, sql, preamble=None, dry_run=False, use_query_cache=True, is_return_debug_info=False):
        d = dict(sql=_sql_to_hash(sql), preamble=preamble)
        hash_ = alex_leontiev_toolbox_python.utils.string_to_hash(
            json.dumps(d, sort_keys=True, ensure_ascii=True))
        table_name = self._prefix + hash_
        assert len(table_name.split(".")[-1]) < _TABLE_NAME_LENGTH_MAX

        env = {
            "table_name": table_name,
            "sql": sql,
        }
        if preamble is not None:
            env["preamble"] = preamble
        rendered_sql = Template("""
        {{preamble}}
        create or replace table `{{table_name}}` as (
            {{sql}}
        )
        """).render(env)
        try:
            used_bytes = alex_leontiev_toolbox_python.bigquery.query_bytes(
                rendered_sql, self._client)
        except Exception:
            self._logger.error(
                alex_leontiev_toolbox_python.utils.number_lines(rendered_sql))
            raise

        if alex_leontiev_toolbox_python.bigquery.table_exists(table_name, bq_client=self._client) and use_query_cache:
            is_executed = False
            self._logger.warning(
                f"table `{table_name}` exists ==> not recreate")
        else:
            is_executed = True
            if not dry_run:
                self._client.query(rendered_sql).result()
                self._quota_used_bytes += used_bytes
            else:
                self._logger.warning("dry_run")

        r = {
            "sql": sql,
            "rendered_sql": rendered_sql,
            "table_name": table_name,
            "datetime_formatted": datetime.now().strftime(alex_leontiev_toolbox_python.caching.DATETIME_FORMAT),
            "preamble": preamble,
            "used_bytes": used_bytes,
            "is_executed": is_executed,
            "dry_run": dry_run,
            "use_query_cache": use_query_cache,
        }
        for cb in self._post_call_callbacks:
            cb(r)

        return (table_name, r) if is_return_debug_info else table_name
