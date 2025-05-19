"""===============================================================================

        FILE: alex_leontiev_toolbox_python/caching/_sql_to_hash_sqlparse.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-08-30T21:20:11.658163
    REVISION: ---

==============================================================================="""
import sqlparse
import hashlib

_SQL_TOKENS_TO_IGNORE = [
    "Token.Text.Whitespace",
    "Token.Text.Whitespace.Newline",
    "Token.Comment.Single",
    "Token.Comment.Multiline",
]


def sql_to_hash_sqlparse(
    sql, algo="md5", salt=None, ttypes_to_ignore: list[str] = _SQL_TOKENS_TO_IGNORE
):
    m = getattr(hashlib, algo)()
    if salt is not None:
        m.update(str(salt).encode())
    for statement in sqlparse.parse(sql):
        for token in statement.flatten():
            if str(token.ttype) not in ttypes_to_ignore:
                m.update(str(token).encode())
    return m.hexdigest()
