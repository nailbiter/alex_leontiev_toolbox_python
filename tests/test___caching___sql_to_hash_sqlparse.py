"""===============================================================================

        FILE: tests/test___caching__sql_to_hash_sqlparse.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-08-30T21:50:38.783957
    REVISION: ---

==============================================================================="""

import os
from alex_leontiev_toolbox_python.caching._sql_to_hash_sqlparse import sql_to_hash_sqlparse


def test_sql_to_hash_sqlparse():
    sql1 = """
    select 1 as x
    """
    sql2 = """
    --some comment
    select 1 as x
    """
    sql3 = """
    select 2 as x
    """
    assert sql_to_hash_sqlparse(
        sql1) != sql_to_hash_sqlparse(sql2, salt="test")
    assert sql_to_hash_sqlparse(sql1) == sql_to_hash_sqlparse(sql2)
    assert sql_to_hash_sqlparse(sql1) != sql_to_hash_sqlparse(sql3)
