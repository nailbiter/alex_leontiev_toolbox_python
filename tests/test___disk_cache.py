"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test___disk_cache.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-03-11T19:56:52.925230
    REVISION: ---

==============================================================================="""
import logging
import unittest
import mock
from alex_leontiev_toolbox_python.utils.disk_cache import 


class TestDiskCache(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._logger = logging.getLogger(self.__class__.__name__)

    def test_something(self):
        self.assertTrue(1 == 1)
        self.assertEqual(1, 1)
        self.assertNotEqual(1, 2)
