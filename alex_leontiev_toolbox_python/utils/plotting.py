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
    page_key: typing.Optional[typing.Callable] = None,
    is_loud: bool = True,
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
            pl if page_key is None else sorted(pl, key=page_key)
        ):
            page_dict = dict(zip(page_key, page_val))
            distinct_row_values, distinct_col_values = (
                _get_distinct(page_slice, rows),
                _get_distinct(page_slice, cols),
            )
            nr, nc = len(distinct_row_values), len(distinct_col_values)

            if is_loud:
                logging.warning((page_dict, rows, cols, nr, nc))

            fig, axs = plt.subplots(nr=nr, nc=nc, **subplots_kwargs)

            plt.close()
            pdf.savefig(fig)

    if is_open:
        os.system(f"open {fn}")
    return fn


def _get_distinct(df: pd.DataFrame, x: list[str]) -> int:
    return (df[x].drop_duplicates()).values
