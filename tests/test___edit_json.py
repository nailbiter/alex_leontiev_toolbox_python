"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test___edit_json.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-10-17T19:42:43.056524
    REVISION: ---

==============================================================================="""
import logging
from alex_leontiev_toolbox_python.utils.edit_json import edit_json


def test_edit_json_set():
    d = {"a": {"b": 1}}
    d = edit_json(d, dict(c=dict(sep="."), ops=[dict(k="$set", v={"a.b": 2})]))
    assert d == {"a": {"b": 2}}

    d = {"a": {"b": 1}}
    d = edit_json(d, dict(c=dict(sep="."), ops={"$set": {"a.b": 2}}))
    assert d == {"a": {"b": 2}}


def test_edit_json_add():
    d = {"a": {"b": [1]}}
    d = edit_json(d, dict(c=dict(sep="."), ops={"$add": {"a.b": [1]}}))
    assert d == {"a": {"b": [1, 1]}}

    d = {"a": {"b": [1]}}
    d = edit_json(
        d,
        dict(
            c=dict(sep="."), ops=[dict(k="$add", v={"a.b": [1]}, c=dict(only_new=True))]
        ),
    )
    assert d == {"a": {"b": [1]}}


def test_edit_json_remove():
    d = {"a": {"b": [1, 2, 3]}}
    d = edit_json(d, dict(c=dict(sep="."), ops={"$remove": {"a.b": [2, 3]}}))
    assert d == {"a": {"b": [1]}}


def test_edit_json_pop():
    d = {"a": {"b": [1, 2, 3]}}
    d = edit_json(d, dict(c=dict(sep="."), ops={"$pop": {"a.b": None}}))
    assert d == {"a": {}}


# class TestEditJson(unittest.TestCase):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._logger = logging.getLogger(self.__class__.__name__)

#     def test_something(self):
#         self.assertTrue(1 == 1)
#         self.assertEqual(1, 1)
#         self.assertNotEqual(1, 2)
