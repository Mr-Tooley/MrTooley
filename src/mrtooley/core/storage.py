# -*- coding: utf-8 -*-

"""
All about storage.


Storage locations:
- Global
- Workspaces
"""

import re
from typing import Any, Union
from collections.abc import MutableMapping
from abc import ABCMeta, abstractmethod
from mrtooley.core.datatypes import Serializable
from pathlib import Path
from decimal import Decimal
from functools import wraps


NATIVE_DATATYPES = (int, bool, float, str, bytes, type(None),
                    Path, Serializable)  # not: complex, memoryview

# Mutable types and collections are note allowed and will be replaced.
COMPATIBLE_DATATYPES_CONVERSION = {
    bytearray: bytes,
    list: tuple,
    set: tuple,
    frozenset: tuple,
    Decimal: float
}

RE_VALID_KEY = re.compile(r"^([a-zA-Z0-9_.]+)$")
RE_VALID_KEY_PATH = re.compile(r"^([^/][a-zA-Z0-9_.]+)/([a-zA-Z0-9_./]+)$")
PATH_SEP = "/"  # Change RE_VALID_KEY_PATH too


class StorageError(Exception):
    pass


def enable_deep_path_lookup(func):
    @wraps(func)
    def wrapper_get(instance, key: str):
        if RE_VALID_KEY.match(key):
            # Use the valid bare key.
            return func(instance, key)

        if m := RE_VALID_KEY_PATH.match(key):
            # Split path by first seperator.
            path_1 = m.group(1)
            path_r = m.group(2)
            child_instance = func(instance, path_1)

            if not isinstance(child_instance, StorageMapping):
                raise KeyError("Element %s is a value and has no following path structure." % path_1)

            return func(child_instance, path_r)

        raise KeyError()

    @wraps(func)
    def wrapper_set(instance, key: str, value):
        if RE_VALID_KEY.match(key):
            return func(instance, key, value)

        if m := RE_VALID_KEY_PATH.match(key):
            # Split path by first separator.
            path_1 = m.group(1)
            path_r = m.group(2)

            child_value = instance.get(key)
            if isinstance(child_value, StorageMapping):
                func(child_value, path_r, value)
            else:
                # Overwriting existing value!


        raise KeyError("Item not found: %s" % key)

    if func.__name__ == "__getitem__":
        return wrapper_get
    if func.__name__ == "__setitem__":
        return wrapper_set
    raise NotImplementedError("This wrapper only supports the functions __getitem__ and __setitem__.")


class StorageMapping(MutableMapping):
    def __init__(self):
        pass

    def __setitem__(self, key: str, value):
        pass

    def __delitem__(self, key: str):
        pass

    def __getitem__(self, key: str):
        pass

    def __len__(self):
        pass

    def __iter__(self):
        pass

    def flush(self):
        pass

    def unload(self):
        self.flush()

    def __del__(self):
        self.unload()


class StorageBackend(metaclass=ABCMeta):
    """
    Represents a storage backend which offers key-value mapping access.

    Key values are expected to be strings of valid characters.
    Keys can be paths containing valid chars and the path separator.
    Paths are being unfolded recursively.

    Backends have to support basic Python data types and classes
    derived from core.datatypes.Serializable which are converted to bytes.

    sequences: All sequences are converted to tuple to maintain immutability.
    """

    def __init__(self):
        pass

    @classmethod
    def type_convert(cls, data: Any) -> Any:
        if isinstance(data, NATIVE_DATATYPES):
            return data

        converter_func = COMPATIBLE_DATATYPES_CONVERSION.get(type(data))
        if converter_func:
            return converter_func(data)

        return data

    @abstractmethod
    def get_storage_root(self) -> StorageMapping:
        pass

    @abstractmethod
    def flush(self):
        pass

    @abstractmethod
    def unload(self):
        pass
