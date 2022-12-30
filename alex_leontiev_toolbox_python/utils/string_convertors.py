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
