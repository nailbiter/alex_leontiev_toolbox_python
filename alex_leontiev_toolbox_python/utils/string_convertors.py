"""===============================================================================

        FILE: ./alex_leontiev_toolbox_python/utils/string_convertors.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-12-30T21:31:19.116862
    REVISION: ---

==============================================================================="""
import functools
import numpy as np
import re
import copy
import logging
import pandas as pd
import itertools


@functools.lru_cache
def num_to_string(
    x,
    int_part_len=4,
    frac_part_len=0,
    joining_sym="d",
    tol=1e-10,
):
    """
    TODO: handle negative
    """
    if frac_part_len > 0:
        int_part = np.floor(x)
        frac_part = x - int_part
        return joining_sym.join(
            [
                num_to_string(
                    int_part,
                    int_part_len,
                    frac_part_len=0,
                    joining_sym=joining_sym,
                    tol=tol,
                ),
                num_to_string(
                    (frac_part * 10**frac_part_len),
                    frac_part_len,
                    frac_part_len=0,
                    joining_sym=joining_sym,
                    tol=tol,
                ),
            ]
        )
    else:
        assert x >= 0
        assert np.abs(x - np.round(x)) < tol, (x, np.round(x), tol)
        x = np.round(x)
        return str(int(x)).zfill(int_part_len)


@functools.lru_cache
def string_to_num(
    s,
    int_part_len=4,
    frac_part_len=0,
    joining_sym="d",
):
    """
    FIXME: maybe, reintegrate in `num_to_string` via `is_inverse` key
    """
    regex = re.compile(
        r"(\d"
        + f"{{{int_part_len}}})"
        + (
            (joining_sym + r"(\d" + f"{{{frac_part_len}}})")
            if frac_part_len > 0
            else ""
        )
    )
    m = regex.match(s)
    assert m is not None, (regex, s)
    res = int(m.group(1))
    if frac_part_len > 0:
        res += int(m.group(2)) / 10**frac_part_len
    return res


class NameCompressor:
    def __init__(self, sep="_", is_allow_collisions=True, compress_prefixes=[]):
        self._sep = sep
        self._forward_map = {}
        self._is_allow_collisions = is_allow_collisions
        self._logger = logging.getLogger(self.__class__.__name__)
        self._compress_prefixes = compress_prefixes

    def __call__(self, s):
        _s = s
        for p in self._compress_prefixes:
            if _s.startswith(p):
                _s = _s[len(p) :]
        res = "".join([t[0] for t in _s.split(self._sep)])
        if (res in self._forward_map.values()) and (
            (s, res) not in self._forward_map.items()
        ):
            if self._is_allow_collisions:
                self._logger.warning(f'collision for "{s}" ==> "{res}"')
            else:
                assert False, (s, res)
        self._forward_map[s] = res
        return res

    @property
    def forward_map(self):
        return copy.deepcopy(self._forward_map)

    @property
    def backward_map(self):
        return {v: k for k, v in self._forward_map.items()}


def compress_dicts(
    dicts,
    fillna_values={},
    is_prune_same_values=False,
    column_names_compressor=lambda x: x,
    stringifiers={},
    in_processor=lambda x: x,
    is_return_debug_info=False,
    joining_symbol="_",
):
    d = {}

    df = pd.DataFrame(dicts)
    for cn in df.columns:
        if cn in fillna_values:
            df[cn] = df[cn].fillna(fillna_values[cn])
    df = in_processor(df)
    for cn in df.columns:
        df[cn] = df[cn].apply(stringifiers.get(cn, str))

    if is_prune_same_values:
        df.drop(
            columns=[cn for cn in df.columns if df[cn].nunique() == 1], inplace=True
        )

    df.columns = map(column_names_compressor, df.columns)

    rs = df.to_dict(orient="records")
    res = [joining_symbol.join(itertools.chain(*r.items())) for r in rs]
    d["backward_map"] = dict(zip(res, dicts))

    return (res, d) if is_return_debug_info else res
