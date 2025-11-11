import typing
import logging
from google.cloud import bigquery
from alex_leontiev_toolbox_python.bigquery.table_with_index import to_sql
from alex_leontiev_toolbox_python.bigquery import query_bytes
import pandas as pd
import operator
import functools
import subprocess
import json
from jinja2 import Template
from datetime import datetime, date

TESTS = [
    (3, "3"),
    ("a", '"""a"""'),
    (datetime(2025, 11, 7), "datetime(2025, 11, 7, 0, 0, 0)"),
    (date(2025, 11, 7), "date(2025, 11, 7)"),
    ([1, 2, 3], "(1, 2, 3)"),
    (["a", "b", "c"], '("""a""", """b""", """c""")'),
]


def test__to_sql():
    for a, b in TESTS:
        sql_expr = to_sql(a)
        query_bytes(f"select {sql_expr}")
        assert sql_expr == b
    # assert to_sql(3) == "3"
    # assert to_sql("a") == '"""a"""'
    # assert to_sql(datetime(2025, 11, 7)) == "datetime(2025, 11, 7)"
    # assert to_sql(date(2025, 11, 7)) == "date(2025, 11, 7)"
    # assert to_sql([1, 2, 3]) == "(1, 2, 3)"
    # assert to_sql(["a", "b", "c"]) == '("""a""", """b""", """c""")'
