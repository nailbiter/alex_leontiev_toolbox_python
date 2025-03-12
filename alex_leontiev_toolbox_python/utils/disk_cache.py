"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/disk_cache.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-03-11T19:56:29.245344
    REVISION: ---

==============================================================================="""
import typing
import hashlib
import json
import functools
import logging
import pickle
import os
from os import path


class FsCache:
    def __init__(
        self,
        dirname: typing.Optional[str],
        is_loud: bool = False,
        hash_algo: str = "md5",
    ):
        self._hash_algo = hash_algo
        self._logger = logging.getLogger(self.__class__.__name__) if is_loud else None
        self._log(f"cache `{dirname}` is created")
        self._dirname = dirname
        if dirname is not None:
            os.makedirs(dirname, exist_ok=True)

    def _log(self, *args, method="warning", **kwargs) -> None:
        if self._logger is not None:
            return getattr(self._logger, method)(*args, **kwargs)

    def __call__(self, f: typing.Callable):
        @functools.wraps(f)
        def _f(*args, **kwargs):
            sha = getattr(hashlib, self._hash_algo)(
                json.dumps(
                    dict(args=args, kwargs=kwargs), sort_keys=True, ensure_ascii=True
                ).encode("utf-8")
            ).hexdigest()
            file_name = path.join(self._dirname, f"{sha}.pkl")
            if path.isfile(file_name):
                self._log(f"cache hit with {file_name}")
                return load_from_pickle(file_name, print_cb=self._log)
            else:
                self._log(f"cache miss with {file_name}")
                res = f(*args, **kwargs)
                save_to_pickle(res, file_name, print_cb=self._log)
                return res

        return _f


def save_to_pickle(data, filename, print_cb: typing.Callable = print) -> None:
    """Saves a Python object to a pickle file."""
    try:
        with open(filename, "wb") as file:
            pickle.dump(data, file)
        print_cb(f"Data successfully saved to {filename}")
    except Exception as e:
        print_cb(f"Error saving to {filename}: {e}")


def load_from_pickle(filename: str, print_cb: typing.Callable = print):
    """Loads a Python object from a pickle file."""
    try:
        with open(filename, "rb") as file:
            data = pickle.load(file)
        print_cb(f"Data successfully loaded from {filename}")
        return data
    except FileNotFoundError:
        print_cb(f"Error: File {filename} not found.")
        return None
    except Exception as e:
        print_cb(f"Error loading from {filename}: {e}")
        return None


# Example usage:
# my_data = {"name": "Coding Partner", "version": 1.0, "items": [1, 2, 3]}
# save_to_pickle(my_data, "my_data.pkl")

# loaded_data = load_from_pickle("my_data.pkl")
# if loaded_data:
#     print("Loaded data:", loaded_data)

# #Example of loading a file that doesn't exist.
# loaded_non_existent = load_from_pickle("does_not_exist.pkl")
