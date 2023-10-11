"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/logging_helpers/decorators/__init__.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-10-11T20:58:06.893477
    REVISION: ---

==============================================================================="""

import functools
import logging
from jinja2 import Template


class deprecated:
    def __init__(self, msg: str = None):
        self._msg = msg

    def __call__(self, f):
        @functools.wraps(f)
        def f_(*args, **kwargs):
            logging.warning(
                Template(
                    "`{{name}}` is deprecated!{%if msg is not none%} {{msg}}{%endif%}"
                ).render(dict(name=f.__name__, msg=self._msg))
            )
            return f(*args, **kwargs)

        return f_
