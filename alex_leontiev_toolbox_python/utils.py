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


def string_to_hash(s, algo="md5"):
    assert algo in ["md5", "sha256"]
    return getattr(hashlib, algo)(s.encode()).hexdigest()


def format_bytes(b, unit="gib"):
    _UNITS = {
        **{f"{w}ib": 2**(10*(i+1)) for i, w in enumerate(list("kmgtp"))}
    }
    assert unit in _UNITS
    return f"{b/_UNITS[unit]:.2f}{unit}"


def number_lines(txt, start_count=0, sep=": "):
    lines = txt.split("\n")
    lines = [s.strip() for s in lines]
    num_digits = int(np.floor(np.log10(len(lines)))+1)
    return "\n".join([f"{str(i+start_count).zfill(num_digits)}{sep}{line}" for i, line in enumerate(lines)])
