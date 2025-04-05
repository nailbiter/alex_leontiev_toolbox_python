"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test___bigquery___table_with_index.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-12-25T21:26:29.928133
    REVISION: ---

==============================================================================="""
import logging
import unittest
from alex_leontiev_toolbox_python.bigquery.table_with_index import (
    TableWithIndex,
    to_sql,
    to_list,
)


def test_basic():
    assert to_sql(3) == "3"
    assert to_sql(True) == "True"
    assert to_sql("x") == '"""x"""'

    assert to_list([1, 2, 3]) == [1, 2, 3]
    assert to_list(1) == [1]
    assert to_list(["x"]) == ["x"]


def test_slice():
    try:
        pass
    finally:
        pass


# class TestBigqueryTableWithIndex(unittest.TestCase):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._logger = logging.getLogger(self.__class__.__name__)

#     def test_something(self):
#         self.assertTrue(1 == 1)
#         self.assertEqual(1, 1)
#         self.assertNotEqual(1, 2)
