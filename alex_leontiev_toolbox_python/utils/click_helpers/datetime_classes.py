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
from datetime import datetime, timedelta, date
import logging
import re
import functools

import click
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

    name = "simple_cli_datetime"

    def __init__(
        self,
        ## https://click.palletsprojects.com/en/7.x/parameters/#parameter-types
        formats: list[str] = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"],
        short_dt_types: dict[str, set[str]] = _SHORT_DT_TYPES,
        now: datetime = None,
        is_debug: bool = False,
        caching_settings: typing.Optional[typing.Tuple[str, str]] = None,
    ):
        super().__init__()
        self._short_dt_types = short_dt_types
        self._formats = formats
        self._now = datetime.now() if now is None else now
        self._logger = logging.getLogger(self.__class__.__name__)
        self._is_debug = is_debug
        if caching_settings is not None:
            self._uuid_cacher = UuidCacher(*caching_settings)
        else:
            self._uuid_cacher = None

    def convert(self, value, param, ctx):
        self._logger.warning((value, param, ctx))

        if self._uuid_cacher is not None:
            value, is_fetched = fetch_or_pass(
                value, self._uuid_cacher, add_on_fail=False
            )
            if is_fetched:
                return datetime.fromisoformat(value)

        try:
            for fmt in self._formats:
                res = None
                is_ok = False
                try:
                    if fmt == "pandas_to_datetime":
                        res = pd.to_datetime(value)
                        is_ok = True
                    elif fmt == "now":
                        res = self._now
                        is_ok = True
                    else:
                        res = datetime.strptime(value, fmt)
                        is_ok = True
                    if fmt in self._short_dt_types:
                        res = res.replace(
                            **{
                                k: getattr(self._now, k)
                                for k in self._short_dt_types[fmt]
                            }
                        )
                except ValueError as ve:
                    if self._is_debug:
                        self._logger.error(ve)

                if is_ok:
                    if self._uuid_cacher is not None:
                        self._uuid_cacher.add(res.isoformat())
                    return res
            if self._is_debug:
                self._logger.error("here")
            raise Exception(dict(value=value, formats=self._formats))
        except Exception as e:
            if self._is_debug:
                self._logger.error(e)
            self.fail(str(dict(value=value, formats=self._formats)), param, ctx)
            # raise e


class ConvenientCliDatetimeParamType(click.ParamType):
    name = "convenient_cli_datetime"

    def convert(self, value, param, ctx):
        return parse_cmdline_datetime(
            value, fail_callback=lambda msg: self.fail(msg, param, ctx)
        )


CLI_DATETIME = ConvenientCliDatetimeParamType()

TIMEDELTA_ABBREVIATIONS = {
    ## idea: match with codes in https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    **{s[0].upper(): f"{s}s" for s in ["minute", "hour", "years"]},
    **{s[0].lower(): f"{s}s" for s in ["day", "month"]},
}


class ConvenientCliTimeParamType(click.ParamType):
    """
    #deprecated; use `ConvenientCliDatetimeParamType` instead
    """

    name = "convenient_cli_time"

    def __init__(self, now=datetime.now()):
        self._now = now

    def convert(self, value, param, ctx):
        # return _common.parse_cmdline_datetime(
        #     value, fail_callback=lambda msg: self.fail(msg, param, ctx)
        # )
        if (
            m := re.match(
                r"\+([\d]+)([" + "".join(TIMEDELTA_ABBREVIATIONS) + "])$", value
            )
        ) is not None:
            res = self._now + relativedelta(
                **{TIMEDELTA_ABBREVIATIONS[m.group(2)]: int(m.group(1))}
            )
        elif (m := re.match(r"(\d{2}):(\d{2})$", value)) is not None:
            res = self._now.replace(
                **{k: int(m.group(i + 1)) for i, k in enumerate(["hour", "minute"])},
            )
        else:
            res = pd.to_datetime(value, errors="coerce")
            if pd.isna(res):
                self.fail(f'cannot parse "{value}"', param, ctx)

        return date_to_grid(res)


CLI_TIME = ConvenientCliTimeParamType


@functools.cache
def next_work_day(dt: datetime, inc: int = 1) -> datetime:
    """
    FIXME: current runtime O(n) is pathetic, make it O(1)
    """
    if inc < 0:
        raise NotImplementedError("FIXME: enable negative increment")
    while inc > 0:
        dt += timedelta(days=1)
        if dt.isoweekday() in list(range(1, 6)):
            inc -= 1
    return dt


DEFAULT_STRPTIME_REGEXPS = [
    (r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}", "%Y-%m-%d %H:%M"),
    (r"\d{4}-\d{2}-\d{2}", "%Y-%m-%d"),
]


def parse_cmdline_datetime(
    s: str,
    fail_callback: typing.Optional[typing.Callable] = None,
    now: typing.Optional[datetime] = None,
    strptime_regexps: list[typing.Tuple[str, str]] = DEFAULT_STRPTIME_REGEXPS,
) -> typing.Optional[datetime]:
    now = datetime.now() if now is None else now
    try:
        if s is None:
            return None
        elif s == "tomorrow":
            res = now.date() + timedelta(days=1)
        elif s == "yesterday":
            res = now.date() - timedelta(days=1)
        elif s == "today":
            res = now.date()
            res = datetime(**{k: getattr(res, k) for k in "year,month,day".split(",")})
        elif (m := re.match(r"next (mon|tue|wed|thu|fri|sat|sun)", s)) is not None:
            weekday = "mon|tue|wed|thu|fri|sat|sun".split("|").index(m.group(1))
            res = now.date() + timedelta(days=1)
            while res.weekday() != weekday:
                res += timedelta(days=1)
        elif (m := re.match(r"last (mon|tue|wed|thu|fri|sat|sun)", s)) is not None:
            weekday = "mon|tue|wed|thu|fri|sat|sun".split("|").index(m.group(1))
            res = now.date() - timedelta(days=1)
            while res.weekday() != weekday:
                res -= timedelta(days=1)
        elif (m := re.match(r"([\+-])(\d+)d", s)) is not None:
            res = now.date()
            res += (1 if m.group(1) == "+" else -1) * timedelta(days=int(m.group(2)))
        else:
            res = None
            for regex, strptime_expression in strptime_regexps:
                m = re.match(regex, s)
                if m is not None:
                    res = datetime.strptime(strptime_expression)
                    break
            if res is None:
                raise NotImplementedError(f'cannot parse parse_cmdline_datetime "{s}"')
        return date_to_grid(res)
    except Exception:
        if fail_callback is not None:
            fail_callback(f"cannot parse {s}")
        raise


@functools.singledispatch
def date_to_grid(dt, grid_minutes: bool = True) -> datetime:
    pass


@date_to_grid.register
def _(dt: datetime, grid_minutes: bool = True) -> datetime:
    kw = dict(second=0, microsecond=0)
    if grid_minutes:
        kw["hour"] = 0
        kw["minute"] = 0
    return dt.replace(**kw)


@date_to_grid.register
def _(dt: date, grid_minutes: bool = True) -> datetime:
    return datetime(year=dt.year, month=dt.month, day=dt.day)
