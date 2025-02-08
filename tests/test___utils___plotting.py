"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/tests/test___utils___plotting.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-09-27T21:37:43.575304
    REVISION: ---

==============================================================================="""
import logging
import unittest
from alex_leontiev_toolbox_python.utils.plotting import plot_to_pdf
from alex_leontiev_toolbox_python.utils.debug_helpers import LogDecorator

# from unittest.mock import MagicMock, Mock
from unittest.mock import Mock, call
import numpy as np
import pandas as pd
import itertools


def _get_df() -> pd.DataFrame:
    df = pd.DataFrame(
        itertools.product(range(10), range(2), range(2)), columns=["p", "r", "c"]
    )
    df[["x", "y"]] = np.random.uniform(0, 1, (len(df), 2))
    return df


def test_plot_to_pdf_1():
    df = _get_df()
    pdf = Mock()
    plotter = Mock(return_value=None)

    plot_to_pdf(
        df,
        plotter=plotter,
        pages=["p"],
        rows=["r"],
        cols=["c"],
        fn=pdf,
        is_open=False,
        # tqdm_factory=tqdm.tqdm,
        is_loud=True,
    )

    pdf.savefig.assert_called()
    plotter.assert_called()


def test_plot_to_pdf_2():
    df = _get_df()
    pdf = Mock()
    plotter = Mock(return_value=None)
    page_sort_key = Mock(side_effect=LogDecorator()(lambda t: t[0]))

    if not True:
        original_groupby = df.groupby

        def _pseudo_groupby(*args, **kwargs):
            # assert False
            logging.warning(("groupby", args, kwargs))
            res = original_groupby(*args, **kwargs)
            logging.warning(list(res))
            return res

        df.groupby = _pseudo_groupby
        df.copy = lambda: df
        logging.warning(df.groupby)

    plot_to_pdf(
        df,
        plotter=plotter,
        pages=["p"],
        page_sort_key=page_sort_key,
        rows=["r"],
        cols=["c"],
        fn=pdf,
        is_open=False,
        # tqdm_factory=tqdm.tqdm,
        is_loud=True,
    )

    pdf.savefig.assert_called()
    logging.warning(page_sort_key.mock_calls)

    # for p in df["p"].unique():
    #     kwarg = (p,)
    #     kwargs = (kwarg,)
    #     logging.warning(kwarg)
    #     page_sort_key.assert_called_with(*kwargs)
    page_sort_key.assert_has_calls(
        [call((p,)) for p in df["p"].unique()], any_order=True
    )
    plotter.assert_called()
