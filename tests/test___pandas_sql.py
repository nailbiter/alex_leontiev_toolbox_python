"""===============================================================================

        FILE: tests/test___pandas_sql.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION:

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION:
     VERSION: ---
     CREATED: 2022-09-06T23:32:28.768229
    REVISION: ---

==============================================================================="""

from alex_leontiev_toolbox_python.pandas_sql import pandas_sql
import pandas as pd
import numpy as np
import string


def test_pandas_sql():
    np.random.seed(42)
    df1 = pd.DataFrame(np.random.randn(10000, 5),
                       columns=list(string.ascii_uppercase[:5]))
    df2 = pd.DataFrame(np.random.randn(10000, 5),
                       columns=list(string.ascii_uppercase[5:10]))
    df = pandas_sql("""
    with t as (
        select sum(A) s
        from tn1
    ), t2 as (
        select sum(F) s
        from tn2
    )
    select s
    from (
        select * from t
        union all
        select * from t2
    )
    """, {"tn1": df1, "tn2": df2})
    assert list(df.columns) == ["s"]
    assert list(df.index) == [0, 1]
    np.linalg.norm(df.values-[[2.28467531e-02],
                              [5.16989643e+01]]) < 5e-8
