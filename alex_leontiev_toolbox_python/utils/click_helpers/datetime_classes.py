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

_SHORT_DT_TYPES_SET = {
    "%H:%M",
}


class SimpleCliDatetimeParamType(click.ParamType):
    """
    intented to be as similar as possible to click.DateTime,
    but with a few quirks:
    1.
    """

    name = "convenient_cli_datetime"

    def __init__(self, *formats, now=None):
        super().__init__()
        if len(formats) == 0:
            ## https://click.palletsprojects.com/en/7.x/parameters/#parameter-types
            formats = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]
        self._formats = formats
        self._now = datetime.now() if now is None else now

    def convert(self, value, param, ctx):
        try:
            for fmt in self._formats:
                try:
                    res = datetime.strptime(value, fmt)
                    if fmt in _SHORT_DT_TYPES_SET:
                        res = res.replace(
                            **{
                                k: geattr(self._now, k)
                                for k in ["year", "month", "day"]
                            }
                        )
                    return res
                except ValueError as ve:
                    pass
            raise Exception(dict(value=value, formats=self._formats))
        except Exception as e:
            self.fail(str(dict(value=value, formats=self._formats)), param, ctx)
            # raise e
