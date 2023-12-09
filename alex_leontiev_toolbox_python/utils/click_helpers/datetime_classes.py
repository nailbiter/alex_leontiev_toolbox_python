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

TODO:
  1. unify "date" and "datetime" via constructor argument
  2. support "next mon", "today", "tomorrow" and etc.
  3. replace in all codes (gstasks, for)
  4. 
==============================================================================="""
import click
from datetime import datetime, timedelta
import logging
import pandas as pd
from alex_leontiev_toolbox_python.utils.click_helpers.cached_classes import (
    UuidCacher,
    fetch_or_pass,
)
import typing

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
        caching_settings: typing.Optional[typing.Tuple[str, str]] = None,
        impute_none: bool = False,
    ):
        super().__init__()
        self._short_dt_types = short_dt_types
        self._formats = formats
        self._now = datetime.now() if now is None else now
        self._impute_none = impute_none
        self._logger = logging.getLogger(self.__class__.__name__)
        self._is_debug = is_debug
        if caching_settings is not None:
            self._uuid_cacher = UuidCacher(*caching_settings)
        else:
            self._uuid_cacher = None

    def convert(self, value, param, ctx):
        logging.warning((value, param, ctx))
        if (value is None) and self._impute_none:
            assert "pandas_to_datetime" in self._formats, self._formats
            value = self._now.isoformat()

        if self._uuid_cacher is not None:
            value = fetch_or_pass(value, self._uuid_cacher)

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
