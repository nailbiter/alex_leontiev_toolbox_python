import typing
import logging
from google.cloud import bigquery
from alex_leontiev_toolbox_python.bigquery.table_with_index import to_sql
import pandas as pd
import operator
import functools
import subprocess
import json
from jinja2 import Template
from datetime import datetime, date


def test__to_sql():
    assert to_sql(3) == "3"
    assert to_sql("a") == '"""a"""'
    assert to_sql(datetime(2025, 11, 7)) == "datetime(2025, 11, 7)"
    assert to_sql(date(2025, 11, 7)) == "date(2025, 11, 7)"
    assert to_sql([1, 2, 3]) == "(1, 2, 3)"
    assert to_sql(["a", "b", "c"]) == '("""a""", """b""", """c""")'
