"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/click_helpers/datetime_classes.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-10-10T19:03:22.450648
    REVISION: ---

==============================================================================="""
import click
from datetime import datetime, timedelta
import logging
import pandas as pd

_SHORT_DT_TYPES = {
    "%H:%M": {"year", "month", "day"},
}


class SimpleCliDatetimeParamType(click.ParamType):
    """
    intented to be as similar as possible to click.DateTime,
    but with a few quirks:
    1.
    """

    name = "convenient_cli_datetime"

    def __init__(
        self,
        ## https://click.palletsprojects.com/en/7.x/parameters/#parameter-types
        formats: list[str] = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"],
        short_dt_types: dict[str, set[str]] = _SHORT_DT_TYPES,
        now: datetime = None,
        is_debug: bool = False,
    ):
        super().__init__()
        self._short_dt_types = short_dt_types
        self._formats = formats
        self._now = datetime.now() if now is None else now
        self._logger = logging.getLogger(self.__class__.__name__)
        self._is_debug = is_debug

    def convert(self, value, param, ctx):
        try:
            for fmt in self._formats:
                try:
                    if fmt == "pandas_to_datetime":
                        res = pd.to_datetime(value)
                    else:
                        res = datetime.strptime(value, fmt)
                    if fmt in self._short_dt_types:
                        res = res.replace(
                            **{
                                k: getattr(self._now, k)
                                for k in self._short_dt_types[fmt]
                            }
                        )
                    return res
                except ValueError as ve:
                    if self._is_debug:
                        self._logger.error(ve)
            if self._is_debug:
                self._logger.error("here")
            raise Exception(dict(value=value, formats=self._formats))
        except Exception as e:
            if self._is_debug:
                self._logger.error(e)
            self.fail(str(dict(value=value, formats=self._formats)), param, ctx)
            # raise e
