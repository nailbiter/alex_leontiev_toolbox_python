"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/bigquery/parse_utils.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-01-21T23:30:20.042646
    REVISION: ---

==============================================================================="""

import sqlparse
import typing
import logging


def query_to_subqueries(sql: str, is_loud: bool = False) -> dict:
    (statement,) = sqlparse.parse(sql)
    t, *_ = [t for t in statement.tokens if t.__class__.__name__ == "IdentifierList"]
    if is_bool:
        logging.warning(t)
    identifiers = [t_ for t_ in t.tokens if t_.__class__.__name__ == "Identifier"]
    if is_bool:
        logging.warning(identifiers)
    d = {i.tokens[0].value: i.tokens[-1].value[1:-1].strip() for i in identifiers}
    return d
