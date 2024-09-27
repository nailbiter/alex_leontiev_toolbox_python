"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/plotting.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-08-08T20:44:33.298593
    REVISION: ---

==============================================================================="""

import pandas as pd
import typing
import matplotlib.pyplot as plt
import seaborn as sns
import uuid
import os
from matplotlib.backends.backend_pdf import PdfPages
import logging
import functools
import itertools
import numpy as np


@functools.singledispatch
def listify(x) -> list:
    return [x]


@listify.register
def _(x: list) -> list:
    return x


@listify.register
def _(x: tuple) -> list:
    return list(x)


@functools.singledispatch
def filename_or_pdf_to_pdf(fn):
    return fn, False


@filename_or_pdf_to_pdf.register
def _(fn: str):
    return PdfPages(fn), True


def plot_to_pdf(
    df: pd.DataFrame,
    plotter: typing.Callable,
    fn: typing.Optional[str] = None,
    pages: list[str] = [],
    rows: list[str] = [],
    cols: list[str] = [],
    subplots_kwargs: dict = {},
    is_open: bool = False,
    tqdm_factory: typing.Callable = lambda x: x,
    # FIXME: fix types of `_key`s
    page_sort_key: typing.Optional[typing.Callable] = -1,
    row_sort_key: typing.Optional[typing.Callable] = -1,
    col_sort_key: typing.Optional[typing.Callable] = -1,
    is_loud: bool = True,
    post_process_fig: typing.Callable = lambda **_: None,
) -> str:
    fn = f"/tmp/{uuid.uuid4()}.pdf" if fn is None else fn
    df = df.copy()

    if len(pages) == 0:
        cn = uuid.uuid4()
        pages = [cn]
        df[cn] = 1
    if len(rows) == 0:
        cn = uuid.uuid4()
        rows = [cn]
        df[cn] = 1
    if len(cols) == 0:
        cn = uuid.uuid4()
        cols = [cn]
        df[cn] = 1

    pdf, is_created_by_us = filename_or_pdf_to_pdf(fn)
    pl = list(df.groupby(pages))
    for page_val, page_slice in tqdm_factory(
        pl if page_sort_key == -1 else sorted(pl, key=page_sort_key)
    ):
        page_val = listify(page_val)
        page_dict = dict(zip(pages, page_val))

        distinct_row_values, distinct_col_values = (
            _get_distinct(page_slice, rows),
            _get_distinct(page_slice, cols),
        )
        if row_sort_key != -1:
            distinct_row_values = sorted(distinct_row_values, key=row_sort_key)
        if col_sort_key != -1:
            distinct_col_values = sorted(distinct_col_values, key=col_sort_key)

        nrows, ncols = len(distinct_row_values), len(distinct_col_values)

        if is_loud:
            logging.warning((page_dict, rows, cols, nrows, ncols))

        fig, axs = plt.subplots(
            **{"nrows": nrows, "ncols": ncols, "squeeze": False, **subplots_kwargs}
        )
        if is_loud:
            logging.warning(axs)

        axs_d = {}
        for (row_val, col_val), ax in zip(
            itertools.product(distinct_row_values, distinct_col_values),
            axs.flatten(),
        ):
            axs_d[(tuple(row_val), tuple(col_val))] = ax

        if is_loud:
            logging.warning(axs_d)

        for v, slc in page_slice.groupby([*rows, *cols]):
            rv, cv = tuple(v[: len(rows)]), tuple(v[len(rows) :])
            if is_loud:
                logging.warning((rv, cv))
            plotter(
                ax=axs_d[rv, cv],
                row_dict=dict(zip(rows, listify(rv))),
                col_dict=dict(zip(cols, listify(cv))),
                data=slc,
                page_dict=page_dict,
            )

        post_process_fig_res = post_process_fig(
            fig=fig,
            page_dict=page_dict,
            page_slice=page_slice,
        )

        post_process_fig_res = (
            (True, True) if post_process_fig_res is None else post_process_fig_res
        )
        is_close, is_should_continue = post_process_fig_res

        if is_close:
            plt.close()
        pdf.savefig(fig)

        if not is_should_continue:
            break

    if is_created_by_us:
        pdf.close()

    if is_open:
        os.system(f"open {fn}")
    return fn


def _get_distinct(df: pd.DataFrame, x: list[str]) -> int:
    return (df[x].drop_duplicates()).values
