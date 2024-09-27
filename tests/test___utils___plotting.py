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

# from unittest.mock import MagicMock, Mock
from unittest.mock import Mock
import numpy as np
import pandas as pd
import itertools


def test_plot_to_pdf():
    df = pd.DataFrame(
        [
            dict(p=p, r=r, c=c)
            for p, r, c in itertools.product(range(10), range(2), range(2))
        ]
    )
    df[["x", "y"]] = np.random.uniform(0, 1, (len(df), 2))

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


# class TestUtilsPlotting(unittest.TestCase):
#     def __init__(self, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self._logger = logging.getLogger(self.__class__.__name__)

#     def test_something(self):
#         self.assertTrue(1 == 1)
#         self.assertEqual(1, 1)
#         self.assertNotEqual(1, 2)
