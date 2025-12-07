"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test___utils___misc_offline.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-12-07T13:19:29.570242
    REVISION: ---

==============================================================================="""
import logging
import unittest

import numpy as np
import pandas as pd

from alex_leontiev_toolbox_python.utils import typify


# class TestUtilsMiscOffline(unittest.TestCase):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._logger = logging.getLogger(self.__class__.__name__)

#     def test_something(self):
#         self.assertTrue(1 == 1)
#         self.assertEqual(1, 1)
#         self.assertNotEqual(1, 2)


def test__typify():
    assert typify(3) == "int"
    assert typify(3.5) == "float"
    assert typify([1, 2, 3]) == "list"
    assert typify(False) == "bool"
    assert typify(True) == "bool"
    # assert typify(np.array([1, 2, 3])) == "list" #breaks
