"""===============================================================================

        FILE: alex_leontiev_toolbox_python/utils.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-07-24T14:21:29.570494
    REVISION: ---

==============================================================================="""
import collections
import functools
import hashlib
import logging
import os
import sqlite3
import string
import time
import typing
import uuid
from datetime import datetime, timedelta
from os import path

import numpy as np
import pandas as pd


def string_to_hash(s, algo="md5"):
    assert algo in ["md5", "sha256"]
    return getattr(hashlib, algo)(s.encode()).hexdigest()


_BYTES_UNITS = {
    **{
        (w if w == "b" else f"{w}ib"): 2 ** (10 * i)
        for i, w in enumerate(list("bkmgtp"))
    }
}


def format_bytes(b: int, unit: typing.Optional[str] = None, is_raw: bool = False):
    assert unit in _BYTES_UNITS or unit is None, (unit, _BYTES_UNITS)
    if unit is None:
        for k, v in _BYTES_UNITS.items():
            unit = k
            if 1 <= b < 2**10:
                break
            b /= 2**10
    else:
        b /= _BYTES_UNITS[unit]
    return (b, unit) if is_raw else f"{b:.2f}{unit}"


def number_lines(txt, start_count=0, sep=": ", start=None, end=None, is_strip=False):
    lines = txt.split("\n")
    if is_strip:
        lines = [s.strip() for s in lines]
    num_digits = int(np.floor(np.log10(len(lines))) + 1)
    lines = list(enumerate(lines))
    if end is None and start is not None:
        lines = lines[start:]
    elif end is not None and start is None:
        lines = lines[:end]
    elif end is not None and start is not None:
        lines = lines[start:end]
    return "\n".join(
        [f"{str(i+start_count).zfill(num_digits)}{sep}{line}" for i, line in lines]
    )


def format_coverage(
    a,
    b,
    is_apply_len=False,
    is_inverse=False,
    equality_sign=" = ",
    slash_sign="/",
    len_f=len,
):
    if is_apply_len:
        a, b = len_f(a), len_f(b)
    assert 0 <= a <= b
    if is_inverse:
        a = b - a
    return f"{a}{slash_sign}{b}{equality_sign}{a/b*100:04.2f}%"


def df_count(
    df, fields, cnt_field_name="cnt", is_normalize_keys=False, is_set_index=False
):
    if is_normalize_keys:
        fields = sorted(set(fields))
    assert cnt_field_name not in fields
    df = pd.DataFrame(
        [
            {
                **{
                    k: v
                    for k, v in (zip(fields, values if len(fields) > 1 else [values]))
                },
                cnt_field_name: len(slice_),
            }
            for values, slice_ in df.groupby(fields)
        ]
    )
    if is_set_index:
        df = df.set_index(fields)
    return df


def df_frac(
    df,
    cnt_field_name="cnt",
    frac_field_name=None,
    is_return_percent=True,
    is_format=False,
    stratification=None,
    is_inplace: bool = False,
    is_return_debug_info: bool = False,
    post_process: typing.Optional[typing.Callable] = None,
):
    """
    1(done). support stratification
    1(done). support copy
    1(done). test
    """
    if not is_inplace:
        df = df.copy()
    if frac_field_name is None:
        frac_field_name = f"frac({cnt_field_name}"
        if stratification is not None:
            frac_field_name += f"|{','.join(stratification)}"
        frac_field_name += ")"
        if is_return_percent:
            frac_field_name += " %"
    if stratification is None:
        df[frac_field_name] = df[cnt_field_name] / df[cnt_field_name].sum()
    else:
        df[frac_field_name] = df[cnt_field_name] / df.groupby(stratification)[
            cnt_field_name
        ].transform("sum")
    if is_return_percent:
        df[frac_field_name] *= 100
    if is_format:
        df[frac_field_name] = df[frac_field_name].apply(lambda x: f"{x:.2f}")
        if is_return_percent:
            df[frac_field_name] = df[frac_field_name] + "%"
    elif post_process is not None:
        df[frac_field_name] = df[frac_field_name].apply(post_process)

    d = {
        "frac_field_name": frac_field_name,
    }

    return (df, d) if is_return_debug_info else df


def is_pandas_superkey(
    df, candidate_superkey, is_normalize_keys=True, cnt_field_name="___cnt"
):
    """
    FIXME: field_name make smarter default, bulletproof and remove from kwargs
    """
    if is_normalize_keys:
        candidate_superkey = sorted(set(candidate_superkey))
    return (
        df_count(
            df,
            candidate_superkey,
            is_normalize_keys=is_normalize_keys,
            cnt_field_name=cnt_field_name,
        )[cnt_field_name].max()
        == 1
    )


def continuous_intervals(arr, step=1, is_presort=True):
    if is_presort:
        arr = sorted(set(arr))
    res = None
    for x in arr:
        if res is None:
            res = [{"start": x, "current": x}]
        elif x == res[-1]["current"] + step:
            res[-1]["current"] += step
        else:
            res[-1]["end"] = res[-1].pop("current")
            res.append({"start": x, "current": x})
    if "current" in res[-1]:
        res[-1]["end"] = res[-1].pop("current")
    return res


def composition(f1, f2):
    @functools.wraps(f2)
    def _composition(*args, f1_composition_args=[], f1_composition_kwargs={}, **kwargs):
        return f1(f2(*args, **kwargs), *f1_composition_args, **f1_composition_kwargs)

    _composition.f1 = f1
    _composition.f2 = f2

    return _composition


class TimeItContext:
    """
    FIXME: also implement as a decorator
    FIXME: move to debug/logging package
    (and maybe merge these two)
    """

    def __init__(
        self,
        title,
        is_warning_on_start=False,
        is_warning_on_end=False,
        report_dict=None,
        print_callback: typing.Optional[typing.Callable] = None,
    ):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._title = title
        self._is_warning_on_start = is_warning_on_start
        self._is_warning_on_end = is_warning_on_end
        self._report_dict = report_dict
        self._print_callback = (
            self._logger.warning if print_callback is None else print_callback
        )

    def __enter__(self, *args, **kwargs):
        self._tic = time.time()
        if self._is_warning_on_start:
            self._print_callback(
                f'"{self._title}" started at {str(datetime.fromtimestamp(self._tic))}'
            )

    def __exit__(self, *args, **kwargs):
        self._toc = time.time()
        if self._is_warning_on_end:
            self._print_callback(
                f'"{self._title}" ended at {str(datetime.fromtimestamp(self._toc))}'
            )
        duration_seconds = self._toc - self._tic
        if self._report_dict is not None:
            self._report_dict["duration_seconds"] = duration_seconds
        self._print_callback(
            f'"{self._title}" took {str(timedelta(seconds=duration_seconds))}'
        )

    # @property
    # def report_dict(self) -> dict:
    #     return self._report_dict


_ASSEMBLE_CALL_STATS_DB_FILE_NAME_ENVVAR = "ASSEMBLE_CALL_STATS_DB_FILE_NAME"


def assemble_call_stats(db_file_name=None, coll_name="call_stats"):
    if db_file_name is None:
        db_file_name = os.environ.get(_ASSEMBLE_CALL_STATS_DB_FILE_NAME_ENVVAR)
    if db_file_name is None:
        logging.warning(
            "db_file_name is none ==> `assemble_call_stats` will do nothing"
        )

    def consumer(f_):
        @functools.wraps(f_)
        def f(*args, **kwargs):
            tic = time.time()
            res = f_(*args, **kwargs)
            toc = time.time()

            if db_file_name is not None:
                conn = sqlite3.connect(db_file_name)
                pd.DataFrame(
                    [
                        {
                            "name": f.__name__,
                            "dt": datetime.now().isoformat(),
                            "tictoc": toc - tic,
                            "__file__": __file__,
                        }
                    ]
                ).to_sql(coll_name, conn, if_exists="append", index=None)
                conn.close()

            return res

        return f

    return consumer


def get_random_fn(
    ext,
    tmp_dir="/tmp",
):
    assert ext.startswith("."), ext
    return path.join(tmp_dir, f"{uuid.uuid4()}{ext}")


def melt_single_record_df(
    df: pd.DataFrame,
    fields_to_preserve: list = [],
    column_names: (str, str) = ("key", "val"),
) -> pd.DataFrame:
    assert len(df) == 1
    (r,) = df.to_dict(orient="records")
    df = pd.DataFrame(
        [
            dict(zip(column_names, t))
            for t in r.items()
            if t[0] not in fields_to_preserve
        ]
    )
    for k in fields_to_preserve:
        df[k] = r[k]
    return df


@functools.singledispatch
def typify(x, is_break: bool = True) -> str:
    if is_break:
        raise NotImplementedError(dict(x=x, t=type(x)))
    else:
        return ""


@typify.register
def _(x: int, **_) -> str:
    return "int"


@typify.register
def _(x: float, **_) -> str:
    return "float"


@typify.register
def _(x: list, **_) -> str:
    return "list"


@typify.register
def _(x: bool, **_) -> str:
    return "bool"


# @typify.register
# def _(x: np.array):
#     return "np.array"
