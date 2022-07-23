#!/usr/bin/env python3
"""===============================================================================

        FILE: scripts-build.py

       USAGE: ./scripts-build.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2022-07-23T15:28:36.305818
    REVISION: ---

==============================================================================="""

import click
#from dotenv import load_dotenv
import os
from os import path
import logging
import toml
import subprocess


def _system(cmd):
    logging.warning(f"> {cmd}")
    ec, out = subprocess.getstatusoutput(cmd)
    assert ec == 0, (ec, cmd, out)


@click.command()
def scripts_build():
    config = toml.load("pyproject.toml")
    version = config["tool"]["poetry"]["version"]
    package_name = config["tool"]["poetry"]["name"]
    _system(" && ".join([
        "rm -rf dist",
        "poetry build",
        f"tar xzf dist/{package_name}-{version}.tar.gz -C dist/",
        f"cp dist/{package_name}-{version}/setup.py .",
    ]))


if __name__ == "__main__":
    #    if path.isfile(".env"):
    #        logging.warning("loading .env")
    #        load_dotenv()
    scripts_build()
