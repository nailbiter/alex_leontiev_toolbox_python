"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/click_format_dataframe.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-08-07T21:00:04.740725
    REVISION: ---

==============================================================================="""
import pandas as pd
import operator
from alex_leontiev_toolbox_python.utils import get_random_fn
import click
import logging

AVAILABLE_OUT_FORMATS = ["str", "csv", "json", "html", "plain", "csvfn"]

_DEFAULT_FORMATTERS = {
    "html": operator.methodcaller("to_html"),
}


def build_click_options(f):
    arr = [
        click.option(
            "-o",
            "--out-format",
            type=click.Choice(AVAILABLE_OUT_FORMATS),
            default="plain",
        ),
        click.option("-c", "--column", "columns", multiple=True),
    ]
    for deco in arr[::-1]:
        f = deco(f)
    return f


def apply_click_options(
    df: pd.DataFrame,
    click_kwargs: dict,
    **kwargs: dict,
) -> str:
    if click_kwargs["columns"]:
        logging.warning(df.columns)
        df = df[list(click_kwargs["columns"])]
    return format_df(df, click_kwargs["out_format"], **kwargs)


def format_df(
    df: pd.DataFrame, out_format: str, formatters: dict = {}, **kwargs: dict
) -> str:
    formatters = {**_DEFAULT_FORMATTERS, **formatters}
    if out_format == "plain":
        s = str(df)
    elif out_format == "str":
        s = df.to_string()
    elif out_format == "json":
        s = df.to_json(
            orient="records",  # force_ascii=False
        )
    elif out_format == "csv":
        s = df.to_csv()
    elif out_format == "html":
        s = formatters[out_format](df)
        # logging.warning(f"{len(df)} tasks matched")
    elif out_format == "csvfn":
        s = get_random_fn(".csv")
        df.to_csv(s)
    else:
        raise NotImplementedError(dict(out_format=out_format))

    return s

    # if out_format not in "json html csv".split():
    #    click.echo(f"{len(df)} tasks matched")
