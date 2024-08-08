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


@functools.singledispatch
def listify(x) -> list:
    return [x]


@listify.register
def _(x: list) -> list:
    return x


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
    post_process_fig: typing.Callable = lambda _: None,
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

    with PdfPages(fn) as pdf:
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
            logging.warning(axs)

            for (row_val, col_val), ax in zip(
                itertools.product(distinct_row_values, distinct_col_values),
                axs.flatten(),
            ):
                row_dict = dict(zip(rows, listify(row_val)))
                col_dict = dict(zip(rows, listify(col_val)))
                if is_loud:
                    logging.warning((row_dict, col_dict))
                slice_ = df[
                    [
                        (row == np.array([*row_val, col_val])).all()
                        for row in df[rows + cols].values
                    ]
                ]
                if len(slice_) > 0:
                    plotter(
                        ax=ax,
                        row_dict=row_dict,
                        col_dict=col_dict,
                        df=slice_,
                        page_dict=page_dict,
                    )

            post_process_fig(fig)
            plt.close()
            pdf.savefig(fig)

    if is_open:
        os.system(f"open {fn}")
    return fn


def _get_distinct(df: pd.DataFrame, x: list[str]) -> int:
    return (df[x].drop_duplicates()).values
