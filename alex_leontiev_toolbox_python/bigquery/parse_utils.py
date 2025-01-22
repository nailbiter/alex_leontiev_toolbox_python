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
import re

TABLE_NAME_REGEX_MANUAL = re.compile(
    r"""`([a-z0-9-]{5,29}[a-z0-9]\.[a-zA-Z0-9_]{1,1024}\.[\s\d\w-]{1,1024})`"""
)

TABLE_NAME_REGEX_GEMINI = re.compile(
    ## https://g.co/gemini/share/88c39b5c709c
    # r"""(?i)  # Case-insensitive matching
    # `?  # Optional backtick for standard SQL
    # (?:(?:[a-zA-Z0-9_-]+\.)?[a-zA-Z0-9_-]+\.)?  # Optional project and dataset names
    # [a-zA-Z0-9_-]+\.?  # Table name with optional trailing dot
    # `?  # Optional closing backtick for standard SQL"""
    r"""(?i)`?(?:(?:[a-zA-Z0-9_-]+\.)?[a-zA-Z0-9_-]+\.)?[a-zA-Z0-9_-]+\.?`?"""
)


def query_to_subqueries(sql: str, is_loud: bool = False) -> dict:
    (statement,) = sqlparse.parse(sql)
    t, *_ = [t for t in statement.tokens if t.__class__.__name__ == "IdentifierList"]
    if is_loud:
        logging.warning(t)
    identifiers = [t_ for t_ in t.tokens if t_.__class__.__name__ == "Identifier"]
    if is_loud:
        logging.warning(identifiers)
    d = {i.tokens[0].value: i.tokens[-1].value[1:-1].strip() for i in identifiers}
    return d


def find_table_names_in_sql_source(
    sql_source,
    bq_client=None,
    is_use_bq_client=False,
    table_name_regex=TABLE_NAME_REGEX_MANUAL,
):
    from google.cloud import bigquery

    """
    FIXME:
        1. enable `is_use_bq_client`
    """
    if is_use_bq_client:
        raise NotImplementedError()

    if (bq_client is None) and is_use_bq_client:
        bq_client = bigquery.Client()

    sql_source = sqlparse.format(sql_source, strip_comments=True)

    return {tn for tn in table_name_regex.findall(sql_source)}
