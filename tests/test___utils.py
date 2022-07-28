"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test_utils.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-07-24T14:54:55.304815
    REVISION: ---

==============================================================================="""
import alex_leontiev_toolbox_python.utils
import pandas as pd


def test_format_bytes():
    assert alex_leontiev_toolbox_python.utils.format_bytes(
        1000, unit="kib") == "0.98kib"
    assert alex_leontiev_toolbox_python.utils.format_bytes(
        1000, unit="kib", is_raw=True) == (0.9765625, "kib")


def test_number_lines():
    assert alex_leontiev_toolbox_python.utils.number_lines(
        "a\nb\nc") == "0: a\n1: b\n2: c"
    assert alex_leontiev_toolbox_python.utils.number_lines(
        "a\nb\nc", start_count=1, sep=":") == "1:a\n2:b\n3:c"


def test_format_coverage():
    assert alex_leontiev_toolbox_python.utils.format_coverage(
        3, 5) == "3/5 = 60.00%"
    assert alex_leontiev_toolbox_python.utils.format_coverage(
        3, 5, equality_sign="=") == "3/5=60.00%"
    assert alex_leontiev_toolbox_python.utils.format_coverage(
        3, 5, slash_sign=" / ") == "3 / 5 = 60.00%"


def test_is_pandas_superkey():
    assert alex_leontiev_toolbox_python.utils.is_pandas_superkey(
        pd.DataFrame({"a": [1, 2], "b": [1, 1]}), ["a"])
    assert not alex_leontiev_toolbox_python.utils.is_pandas_superkey(
        pd.DataFrame({"a": [1, 1], "b": [1, 1]}), ["a"])
