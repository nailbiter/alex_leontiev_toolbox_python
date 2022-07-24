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


def test_format_bytes():
    assert alex_leontiev_toolbox_python.utils.format_bytes(
        1000, unit="kib") == "0.98kib"


def test_number_lines():
    assert alex_leontiev_toolbox_python.utils.number_lines(
        "a\nb\nc") == "0: a\n1: b\n2: c"
    assert alex_leontiev_toolbox_python.utils.number_lines(
        "a\nb\nc", start_count=1, sep=":") == "1:a\n2:b\n3:c"
