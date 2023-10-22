"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/logging_helpers/wrapped_logging.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-10-22T10:08:16.219159
    REVISION: ---

==============================================================================="""
import logging
import subprocess
from jinja2 import Template
import click
from requests.auth import HTTPBasicAuth
from alex_leontiev_toolbox_python.utils import get_random_fn
import requests
import typing
from datetime import datetime, timedelta
import functools
import shlex
import logging


class WrappedLogging:
    def __init__(self):
        self._is_configured = False
        self._post_config_messages = []

    def add_post_config_message(self, severity: str, message: str):
        self._post_config_messages.add((severity, message))

    def _flush_post_config_messages(self):
        for severity, message in self._post_config_messages:
            getattr(self, severity)(message)

    def warning(self, *args, **kwargs):
        assert self._is_configured
        return logging.warning(*args, **kwargs)

    def info(self, *args, **kwargs):
        assert self._is_configured
        return logging.info(*args, **kwargs)

    def configure_debug(self, debug: bool, debug_file: str) -> None:
        """
        TODO:
        1(done). convert to class
        2. move to `altp`
        """
        total_level = logging.INFO
        basic_config_kwargs = {"handlers": [], "level": total_level}
        if debug:
            debug_fn = (
                get_random_fn(".log.txt") if (debug_file == "@random") else debug_file
            )
            _handler = logging.FileHandler(filename=debug_fn)
            _handler.setFormatter(
                logging.Formatter(
                    fmt="%(asctime)s,%(msecs)d %(levelname)-8s %(name)s [%(filename)s:%(lineno)d] %(message)s",
                    datefmt="%Y-%m-%d:%H:%M:%S",
                )
            )
            _handler.setLevel(total_level)
            basic_config_kwargs["handlers"].append(_handler)
        _handler = logging.StreamHandler()
        _handler.setLevel(logging.WARNING)
        basic_config_kwargs["handlers"].append(_handler)
        logging.basicConfig(**basic_config_kwargs)
        if debug:
            logging.warning(f'log saved to "{debug_fn}"')

        # if loaded_dotenv is not None:
        #     logging.warning(f'loading "{loaded_dotenv}"')
        self._is_configured = True
        self._flush_post_config_messages()
