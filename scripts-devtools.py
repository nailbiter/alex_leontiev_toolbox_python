#!/usr/local/anaconda3/envs/base-with-altp/bin/python

"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/scripts-devtools.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-04-06T10:49:36.490802
    REVISION: ---

==============================================================================="""

import click

# from dotenv import load_dotenv
import os
from os import path
import logging
from google.cloud import bigquery
import operator
import pandas as pd


@click.group()
def scripts_devtools():
    pass


@scripts_devtools.command()
def check_datasets():
    bq_client = bigquery.Client()
    click.echo(
        pd.DataFrame(
            map(
                lambda d: {k: getattr(d, k) for k in ["dataset_id", "full_dataset_id"]},
                bq_client.list_datasets(),
            )
        )
    )


if __name__ == "__main__":
    #    fn = ".env"
    #    if path.isfile(fn):
    #        logging.warning(f"loading `{fn}`")
    #        load_dotenv(dotenv_path=fn)
    scripts_devtools()
