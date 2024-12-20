"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/bigquery/table_with_index.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-12-20T20:58:47.503127
    REVISION: ---

==============================================================================="""
import typing


class TableWithIndex:
    def __init__(self, table_name: str, index: list[str], is_superkey: typing.Callable):
        assert is_superkey(table_name, index), (table_name, index)
        self._table_name = table_name
        self._index = index

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def index(self) -> list[str]:
        return list(self._index)
