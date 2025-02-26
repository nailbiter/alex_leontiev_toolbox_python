"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test___utils___datetime_classes.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-02-26T18:53:41.305166
    REVISION: ---

==============================================================================="""
import logging
import unittest
from alex_leontiev_toolbox_python.utils.click_helpers.datetime_classes import (
    SimpleCliDatetimeParamType,
    parse_cmdline_datetime,
)
from datetime import datetime


def test_parse_cmdline_datetime():
    now = datetime.strptime("2025-02-26 19:18", "%Y-%m-%d %H:%M")
    assert parse_cmdline_datetime("yesterday", now=now) == datetime(2025, 2, 25)
    assert parse_cmdline_datetime("today", now=now) == datetime(2025, 2, 26)
    assert parse_cmdline_datetime("tomorrow", now=now) == datetime(2025, 2, 27)
    assert parse_cmdline_datetime("next mon", now=now) == datetime(2025, 3, 3)
    assert parse_cmdline_datetime("last mon", now=now) == datetime(2025, 2, 24)


# class TestUtilsDatetimeClasses(unittest.TestCase):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._logger = logging.getLogger(self.__class__.__name__)

#     def test_something(self):
#         self.assertTrue(1 == 1)
#         self.assertEqual(1, 1)
#         self.assertNotEqual(1, 2)
