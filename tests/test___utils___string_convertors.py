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
)


def test_num_to_string():
    assert num_to_string(4, 2) == "04"
    assert string_to_num("04", 2) == 4
    assert num_to_string(4.2, 2, 3) == "04d200"
    assert string_to_num("04d200", 2, 3) == 4.2


def test_compressor():
    pass
