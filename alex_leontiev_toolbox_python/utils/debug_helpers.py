"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/debug_helpers.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-02-08T19:51:19.337968
    REVISION: ---

==============================================================================="""
import logging
import typing


class LogDecorator:
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)

    def __call__(self, f: typing.Callable):
        def _f(*args, **kwargs):
            res = f(*args, **kwargs)
            self._logger.warning(
                f"{f.__name__} called with {dict(args=args,kwargs=kwargs)} and returned {res}"
            )
            return res

        return _f
