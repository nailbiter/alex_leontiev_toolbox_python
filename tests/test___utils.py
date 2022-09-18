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
import numpy as np
import functools


def test_format_bytes():
    assert alex_leontiev_toolbox_python.utils.format_bytes(
        1000, unit="kib") == "0.98kib"
    assert alex_leontiev_toolbox_python.utils.format_bytes(
        1000, unit="kib", is_raw=True) == (0.9765625, "kib")


def test_number_lines():
    assert alex_leontiev_toolbox_python.utils.number_lines(
        "a\nb\nc") == "0: a\n1: b\n2: c"
    assert alex_leontiev_toolbox_python.utils.number_lines(
        "a\nb\nc", end=2) == "0: a\n1: b"
    assert alex_leontiev_toolbox_python.utils.number_lines(
        "a\nb\nc", start=1) == "1: b\n2: c"
    assert alex_leontiev_toolbox_python.utils.number_lines(
        "a\nb\nc", start=1, end=2) == "1: b"
    assert alex_leontiev_toolbox_python.utils.number_lines(
        "a\nb\nc", start_count=1, sep=":") == "1:a\n2:b\n3:c"


def test_format_coverage():
    assert alex_leontiev_toolbox_python.utils.format_coverage(
        3, 5) == "3/5 = 60.00%"
    assert alex_leontiev_toolbox_python.utils.format_coverage(
        3, 5, is_inverse=True) == "2/5 = 40.00%"
    assert alex_leontiev_toolbox_python.utils.format_coverage(
        3, 5, equality_sign="=") == "3/5=60.00%"
    assert alex_leontiev_toolbox_python.utils.format_coverage(
        3, 5, slash_sign=" / ") == "3 / 5 = 60.00%"


def test_is_pandas_superkey():
    assert alex_leontiev_toolbox_python.utils.is_pandas_superkey(
        pd.DataFrame({"a": [1, 2], "b": [1, 1]}), ["a"])
    assert not alex_leontiev_toolbox_python.utils.is_pandas_superkey(
        pd.DataFrame({"a": [1, 1], "b": [1, 1]}), ["a"])


def test_df_frac():
    df = pd.DataFrame({"cnt": [1, 2, 2, 5]})
    df["x"] = df["cnt"]

    dfp = alex_leontiev_toolbox_python.utils.df_frac(df)
    assert set(dfp.columns) == {"x", "cnt", "frac(cnt) %"}
    assert set(df.columns) == {"x", "cnt"}
    assert np.linalg.norm(dfp["frac(cnt) %"] -
                          np.array([10, 20, 20, 50])) < 1e-10

    dfp = alex_leontiev_toolbox_python.utils.df_frac(df, frac_field_name="f")
    assert set(dfp.columns) == {"x", "cnt", "f"}
    assert set(df.columns) == {"x", "cnt"}
    assert np.linalg.norm(dfp["f"] - np.array([10, 20, 20, 50])) < 1e-10

    dfp = alex_leontiev_toolbox_python.utils.df_frac(df, cnt_field_name="x")
    assert set(dfp.columns) == {"x", "cnt", "frac(x) %"}
    assert set(df.columns) == {"x", "cnt"}
    assert np.linalg.norm(dfp["frac(x) %"] -
                          np.array([10, 20, 20, 50])) < 1e-10

    df["f"] = ["a", "a", "a", "b"]
    dfp = alex_leontiev_toolbox_python.utils.df_frac(df, stratification=["f"])
    assert set(dfp.columns) == {"x", "cnt", "frac(cnt|f) %", "f"}
    assert set(df.columns) == {"x", "cnt", "f"}
    assert np.linalg.norm(dfp["frac(cnt|f) %"] -
                          np.array([20, 40, 40, 100])) < 1e-10
    df.pop("f")

    dfp = alex_leontiev_toolbox_python.utils.df_frac(df, is_inplace=True)
    assert set(dfp.columns) == {"x", "cnt", "frac(cnt) %"}
    assert set(df.columns) == set(dfp.columns)
    assert np.linalg.norm(dfp["frac(cnt) %"] -
                          np.array([10, 20, 20, 50])) < 1e-10


def test_continuous_intervals():
    assert alex_leontiev_toolbox_python.utils.continuous_intervals(
        [1, 2, 3, 4, 5]) == [{"start": 1, "end": 5}]
    assert alex_leontiev_toolbox_python.utils.continuous_intervals(
        [1, 2, 4, 5]) == [{"start": 1, "end": 2}, {"start": 4, "end": 5}]
