"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/alex_leontiev_toolbox_python/alex_leontiev_toolbox_python/utils/edit_json.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2023-10-17T19:41:06.684767
    REVISION: ---

==============================================================================="""
import json
import copy
import typing
import functools


def _recursive_set(d: dict, keys: list, val) -> dict:
    if len(keys) == 0:
        return d
    elif len(keys) == 1:
        (key,) = keys
        d[key] = val
        return d
    else:
        key, *_keys = keys
        return _recursive_set(d[key], _keys, val)


@functools.singledispatch
def _apply_operations(operations, src: dict, config: dict):
    raise NotImplementedError()


@_apply_operations.register
def _(operations: dict, src: dict, config: dict):
    return _apply_operations(
        [dict(k=k, v=v) for k, v in operations.items()], src, config
    )


@_apply_operations.register
def _(operations: list, src: dict, config: dict):
    for operation in operations:
        key = operation["k"]
        val = operation.get("v")

        if key == "$set":
            sep = config.get("sep")
            for k, v in val.items():
                if sep is None:
                    src[k] = v
                else:
                    _recursive_set(src, k.split(sep), v)
        else:
            raise NotImplementedError(dict(operation=operation))
    return src


def edit_json(src: dict, patch: dict = {}) -> dict:
    config = patch.get("c", {})
    if config.get("is_copy", False):
        src = copy.deepcopy(src)
    operations = patch.get("ops", [])
    return _apply_operations(operations, src, config)
