"""===============================================================================

        FILE: tests/test___utils___string_convertors.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-12-30T21:32:10.423505
    REVISION: ---

==============================================================================="""

from alex_leontiev_toolbox_python.utils.string_convertors import (
    num_to_string,
    string_to_num,
    NameCompressor,
    compress_dicts,
)
import pytest
import logging
import functools


def test_num_to_string():
    assert num_to_string(4, 2) == "04"
    assert string_to_num("04", 2) == 4
    assert num_to_string(4.2, 2, 3) == "04d200"
    assert string_to_num("04d200", 2, 3) == 4.2


def test_compressor():
    compressor = NameCompressor(is_allow_collisions=False, compress_prefixes=["is_"])
    assert compressor("is_enable_x") == "ex"
    assert compressor("is_enable_x") == "ex"
    assert compressor("correction_coefficient") == "cc"
    assert compressor.forward_map == {
        "is_enable_x": "ex",
        "correction_coefficient": "cc",
    }
    assert compressor.backward_map == {
        "ex": "is_enable_x",
        "cc": "correction_coefficient",
    }
    with pytest.raises(AssertionError) as e:
        s = "cutoff_coefficient"
        res = compressor(s)
        logging.error((s, res))


def test_compress_dicts():
    ds = [
        {
            "is_enable_cutoff": True,
            "coefficient": 5,
        },
        {"clapping_cap": 10.1, "coefficient": 5},
    ]

    cs, d = compress_dicts(
        ds,
        fillna_values={"is_enable_cutoff": False, "clapping_cap": 0},
        column_names_compressor=NameCompressor(compress_prefixes=["is_"]),
        is_return_debug_info=True,
        is_prune_same_values=True,
        stringifiers={
            "clapping_cap": functools.partial(
                num_to_string, int_part_len=2, frac_part_len=1
            ),
            "is_enable_cutoff": {True: "on", False: "off"}.get,
        },
    )

    assert cs == ["ec_on_cc_00d0", "ec_off_cc_10d1"]
    assert d == {
        "backward_map": {
            "ec_on_cc_00d0": {"is_enable_cutoff": True, "coefficient": 5},
            "ec_off_cc_10d1": {"clapping_cap": 10.1, "coefficient": 5},
        }
    }
