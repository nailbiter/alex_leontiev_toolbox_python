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
import hashlib
import numpy as np
import string
import pandas as pd


def string_to_hash(s, algo="md5"):
    assert algo in ["md5", "sha256"]
    return getattr(hashlib, algo)(s.encode()).hexdigest()


def format_bytes(b, unit="gib", is_raw=False):
    _UNITS = {
        **{f"{w}ib": 2**(10*(i+1)) for i, w in enumerate(list("kmgtp"))}
    }
    assert unit in _UNITS

    b /= _UNITS[unit]
    return (b, unit) if is_raw else f"{b:.2f}{unit}"


def number_lines(txt, start_count=0, sep=": ", start=None, end=None):
    lines = txt.split("\n")
    lines = [s.strip() for s in lines]
    num_digits = int(np.floor(np.log10(len(lines)))+1)
    lines = list(enumerate(lines))
    if end is None and start is not None:
        lines = lines[start:]
    elif end is not None and start is None:
        lines = lines[:end]
    elif end is not None and start is not None:
        lines = lines[start:end]
    return "\n".join([f"{str(i+start_count).zfill(num_digits)}{sep}{line}" for i, line in lines])


def format_coverage(a, b, is_apply_len=False, is_inverse=False, equality_sign=" = ", slash_sign="/", len_f=len):
    if is_apply_len:
        a, b = len_f(a), len_f(b)
    assert 0 <= a <= b
    if is_inverse:
        a = b-a
    return f"{a}{slash_sign}{b}{equality_sign}{a/b*100:04.2f}%"


def df_count(df, fields, cnt_field_name="cnt", is_normalize_keys=False, is_set_index=False):
    if is_normalize_keys:
        fields = sorted(set(fields))
    assert cnt_field_name not in fields
    df = pd.DataFrame([
        {**{k: v for k, v in (zip(fields, values if len(fields)
                              > 1 else [values]))}, cnt_field_name: len(slice_)}
        for values, slice_
        in df.groupby(fields)
    ])
    if is_set_index:
        df = df.set_index(fields)
    return df


def df_frac(df, cnt_field_name="cnt", frac_field_name=None, is_return_percent=True, is_format=False, stratification=None, is_inplace=False):
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
        df[frac_field_name] = df[cnt_field_name]/df[cnt_field_name].sum()
    else:
        df[frac_field_name] = df[cnt_field_name] / \
            df.groupby(stratification)[cnt_field_name].transform(np.sum)
    if is_return_percent:
        df[frac_field_name] *= 100
    if is_format:
        df[frac_field_name] = df[frac_field_name].apply(lambda x: f"{x:.2f}")
        if is_return_percent:
            df[frac_field_name] = df[frac_field_name]+"%"
    return df


def is_pandas_superkey(df, candidate_superkey, is_normalize_keys=True, cnt_field_name="___cnt"):
    """
    FIXME: field_name make smarter default, bulletproof and remove from kwargs
    """
    if is_normalize_keys:
        candidate_superkey = sorted(set(candidate_superkey))
    return df_count(df, candidate_superkey, is_normalize_keys=is_normalize_keys, cnt_field_name=cnt_field_name)[cnt_field_name].max() == 1
