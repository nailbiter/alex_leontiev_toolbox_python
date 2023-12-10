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
import logging


def _recursive_apply(
    d: dict, keys: list, val: typing.Any, op: str, config: dict
) -> dict:
    if len(keys) == 0:
        return d
    elif len(keys) == 1:
        (key,) = keys
        _apply(d, key, val, op, config)
        return d
    else:
        key, *_keys = keys
        return _recursive_apply(d[key], _keys, val, op, config)


def _apply(d: dict, key: str, val: typing.Any, op: str, config: dict) -> None:
    if op == "$set":
        d[key] = val
    elif op == "$remove":
        d[key] = [x for x in d[key] if x not in val]
    elif op == "$pop":
        logging.warning((d, key, val, op, config))
        d.pop(key)
        logging.warning(d)
    elif op == "$add":
        l = list(d[key])
        d[key] = l + [
            x for x in val if (x not in l) or (not config.get("only_new", False))
        ]
    else:
        raise NotImplementedError(dict(op=op))


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
        _config = {**config, **operation.get("c", {})}

        if key in ["$set", "$add", "$remove", "$pop"]:
            sep = _config.get("sep")
            for k, v in val.items():
                _recursive_apply(
                    src, [k] if sep is None else k.split(sep), v, op=key, config=_config
                )
        else:
            raise NotImplementedError(dict(operation=operation))
    return src


def edit_json(src: dict, patch: dict = {}) -> dict:
    """
    patch: dict([c=], ops=[op])
    c: [sep='.'],
    op:
      k="$set", v={"a.b": 2}
      k="$add", v={"a.b":[1,2]} [c= {only_new=False}]
    """
    config = patch.get("c", {})
    if config.get("is_copy", False):
        src = copy.deepcopy(src)
    operations = patch.get("ops", [])
    return _apply_operations(operations, src, config)
