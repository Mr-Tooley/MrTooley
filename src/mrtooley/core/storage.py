# -*- coding: utf-8 -*-

"""
All about key-value storage, path resolution logic for keys
"""

import re
from typing import Any, Union, Type
from collections.abc import MutableMapping, Mapping
from abc import ABCMeta, abstractmethod
from functools import wraps
from types import MappingProxyType


EMPTY_MAPPING = MappingProxyType({})

# Every storage backend has to handle these types (or transcode them appropriately)
BASIC_SUPPORTED_DATATYPES: tuple[Type, ...] = (int, bool, float, str, bytes, type(None), Mapping)

RE_VALID_KEY = re.compile(r"^([a-zA-Z0-9_.]+)$")
RE_VALID_PATH_FIRST_REMAIN = re.compile(r"^([a-zA-Z0-9_.]+)/([a-zA-Z0-9_./]+)$")
RE_VALID_PATH_BEGIN_LAST = re.compile(r"^(([^/][a-zA-Z0-9_./]+)/)?([a-zA-Z0-9_.]+)$")
PATH_SEP = "/"  # Change regexes too


class StorageError(Exception):
    """
    A generic storage error
    """


class StorageKeyError(KeyError, StorageError):
    """
    Specialized KeyError but also raises on invalid paths.
    """


class StorageMappingExpected(StorageError):
    """
    Raises when a key path exceeds over a non mapping value.
    m["path/x"]=123
    print(m["path/x/y"])
    """


def make_path_aware(func):
    """
    Decorator for wrapping the magic methods in mapping classes which contain "key":
    __getitem__, __setitem__, __delitem__, __contains__
    This decorater takes care of resolving tree hierarchy paths into flat key-value mapping.
    The StorageMapping implementing subclass
    """
    def unfold_full_path(instance: StorageMapping, path: str) -> Union[Any, StorageMapping]:
        if RE_VALID_KEY.match(path):
            # "simple_key"
            # Use a simple key.
            # Call bare __getitem__
            return func(instance, path)  # Returns a StorageMapping or a value

        if m := RE_VALID_PATH_FIRST_REMAIN.match(path):
            # "some/path/key"
            # Split path by first separator.
            path_1 = m.group(1)
            path_r = m.group(2)

            # Call bare __getitem__
            # instance.__getitem__("some")
            subinstance = func(instance, path_1)

            if not isinstance(subinstance, Mapping):
                raise StorageMappingExpected(f"Got a value instead of a mapping here: {path_1} for path {path}")

            # Recursive call on sub instance
            # Call next deep logic instead of __getitem__
            # subinstance["path/key"]
            return subinstance[path_r]  # Returns a StorageMapping or a value

        # Not a simple key and not a valid path
        raise StorageKeyError("Invalid key or path: " + str(path))

    def unfold_to_last_mapping(instance: StorageMapping, path: str) -> tuple[StorageMapping, str]:
        if m := RE_VALID_PATH_BEGIN_LAST.match(path):
            # "some/path/key"
            # Split path by last separator.
            path_begin = m.group(2)  # "some/path"
            path_key = m.group(3)  # "key"

            if path_begin is None:
                # path = "key"
                # return instance, key
                return instance, path_key

            # path = "path/key"
            # return instance["path"], key
            subinstance = instance[path_begin]
            if not isinstance(subinstance, Mapping):
                raise StorageMappingExpected(f"Got a value instead of a mapping here: '{path_begin}' for path '{path}'")

            return subinstance, path_key

        # Not a simple key and not a valid path
        raise StorageKeyError("Invalid key or path: " + str(path))

    @wraps(func)
    def wrapper_resolve_full_key(instance, key: str):
        # deep/path/key
        # __getitem__(last_instance, "last_key")
        return unfold_full_path(instance, key)

    @wraps(func)
    def wrapper_last_mapping_call_key_value(instance, key: str, value):
        # __setitem__(last_instance, key, value)
        last_instance, subkey = unfold_to_last_mapping(instance, key)

        if not isinstance(value, Mapping):
            res = func(last_instance, subkey, value)
            last_instance.flush()
            return res

        # It's a Mapping. Let the backend create one.
        res = func(last_instance, subkey, EMPTY_MAPPING)

        # Get new StorageMapping instance and copy each item
        new_mapping = last_instance[subkey]
        for k, v in value.items():
            new_mapping[k] = v
        last_instance.flush()
        return res

    @wraps(func)
    def wrapper_last_mapping_call_key(instance, key: str):
        # __delitem__(last_instance, key)
        last_instance, subkey = unfold_to_last_mapping(instance, key)
        res = func(last_instance, subkey)
        last_instance.flush()
        return res

    @wraps(func)
    def wrapper_last_mapping_call_key_supress_exceptions(instance, key: str):
        # Ignore exceptions when checking '"key" in storage'.
        # __contains__(last_instance, key)
        try:
            last_instance, subkey = unfold_to_last_mapping(instance, key)
            res = func(last_instance, subkey)
            last_instance.flush()
            return res
        except StorageError:
            return False

    func_map = {
        "__getitem__": wrapper_resolve_full_key,
        "__setitem__": wrapper_last_mapping_call_key_value,
        "__delitem__": wrapper_last_mapping_call_key,
        "__contains__": wrapper_last_mapping_call_key_supress_exceptions,
    }

    newfunc = func_map.get(func.__name__)
    if newfunc is None:
        raise NotImplementedError("make_path_aware does not support function: " + func.__name__)

    return newfunc


class StorageMapping(MutableMapping, metaclass=ABCMeta):
    # Datatypes the backend supports transparently
    # __setitem__ is being called directly and __getitem__ is expected to return the same datatype back
    NATIVE_DATATYPES: tuple[Type, ...] = ()

    # Remaining datatypes which the backend can take care of
    EXTRA_DATATYPES: tuple[Type, ...] = ()

    def __init__(self):
        pass

    @abstractmethod
    def __setitem__(self, key: str, value):
        """
        Sets or updates a key-value pair.
        Value can be of type BASIC_SUPPORTED_DATATYPES
        """

    @abstractmethod
    def __delitem__(self, key: str):
        """
        Deletes an item.
        """

    @abstractmethod
    def __getitem__(self, key: str) -> Union["StorageMapping", Any]:
        """
        Retrieves a value by it's key.
        Value will be of type BASIC_SUPPORTED_DATATYPES
        """

    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def __contains__(self, key: str):
        pass

    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def flush(self):
        """Implies saving buffers now."""

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def unload(self):
        self.flush()

    def __del__(self):
        try:
            self.unload()
        except Exception as e:
            pass


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

    MAPPING_CLASS = StorageMapping

    def __init__(self):
        pass

    @abstractmethod
    def get_storage_root(self) -> StorageMapping:
        pass

    @abstractmethod
    def unload(self):
        pass

    def __repr__(self):
        return f"{self.__class__.__name__}"


def test_storage_backend(sb: StorageBackend):
    root = sb.get_storage_root()  # type: StorageMapping

    root["key1"] = "value1"
    root["key2"] = "value2"
    root["int"] = 42
    root["float"] = 42.42
    root["bytes"] = b"bytes!"

    for key, val in root.items():
        print(key, val)

    assert root["key1"] == "value1"
    assert root["key2"] == "value2"
    assert root["int"] == 42
    assert type(root["int"]) == int
    assert root["float"] == 42.42
    assert type(root["float"]) == float
    assert root["bytes"] == b"bytes!"
    assert type(root["bytes"]) == bytes

    assert len(root) == 5

    assert "key1" in root
    del root["key1"]

    assert len(root) == 4
    assert "key1" not in root

    assert root.get("key1", "default") == "default"
    assert root.get("key2", "default") == "value2"

    root["paths"] = dict(sub1=1, sub2=2)
    path = root["paths"]
    assert isinstance(path, Mapping)
    assert isinstance(path, StorageMapping)

    assert path["sub1"] == 1
    assert path["sub2"] == 2

    path["a"] = {"very": {"deep": {"path": b"hello world"}}}
    assert root["paths/a/very/deep/path"] == b"hello world"

    root["replacing"] = "string"
    assert root["replacing"] == "string"

    root["replacing"] = {"sub1": 123, "sub2": 456}
    assert isinstance(root["replacing"], Mapping)

    assert root["replacing/sub1"] == 123
    assert root["replacing/sub2"] == 456

    root["replacing"] = "replaced by string"
    assert root["replacing"] == "replaced by string"

    assert "replacing/sub1" not in root
