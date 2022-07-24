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

TODO:
    1.

FIXME:
    1. investigate whether `setup.py` is needed (maybe, only `pyproject.toml` suffices??)

==============================================================================="""

import click
# from dotenv import load_dotenv
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
@click.option("--tag/--no-tag", default=True)
@click.option("--generate-setup/--no-generate-setup", default=False)
def scripts_build(tag, generate_setup):
    assert not tag

    config = toml.load("pyproject.toml")
    version = config["tool"]["poetry"]["version"]
    package_name = config["tool"]["poetry"]["name"]

    if generate_setup:
        pass
#        _system(" && ".join([
#            "rm -rf dist",
#            "poetry build",
#            f"tar xzf dist/{package_name}-{version}.tar.gz -C dist/",
#            f"cp dist/{package_name}-{version}/setup.py .",
#            "git add setup.py",
#        ])
    _system(" && ".join([
        "git commit -a -m \"release {version}\"",
        "git push",
    ]))


if __name__ == "__main__":
    scripts_build()
