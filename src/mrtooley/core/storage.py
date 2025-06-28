# -*- coding: utf-8 -*-

"""
All about key-value storage, path resolution logic for keys
"""

import re
import datetime
from contextlib import suppress
from typing import Any, Union, Type
from collections.abc import MutableMapping, Mapping
from abc import ABCMeta, abstractmethod
from functools import wraps
from types import MappingProxyType
from decimal import Decimal
from mrtooley.core.datatypes import Serializable, Serializer

from mrtooley.core.logger import module_logger
mlogger = module_logger(__name__)

EMPTY_MAPPING = MappingProxyType({})

# Every storage backend has to handle these types (or transcode them appropriately internally)
MINIMAL_SUPPORTED_DATATYPES: tuple[Type, ...] = \
    (int, bool, float, str, bytes, type(None), Mapping)

# Backends can provide these extra datatypes if they can handle them directly or better
OPTIONAL_SUPPORTED_DATATYPES: tuple[Type, ...] = \
    (tuple, Serializable, datetime.date, datetime.time, datetime.datetime, datetime.timedelta)

# Resulting set of supported data types which are guarateed to return in the same format.
SUPPORTED_DATATYPES = MINIMAL_SUPPORTED_DATATYPES + OPTIONAL_SUPPORTED_DATATYPES


# These values are allowed as input but get condensed into a generic type.
# Reading back stored values won't return the original type!
LOSSY_VALUE_CONVERSION = {
    list: tuple,
    set: tuple,
    frozenset: tuple,
    bytearray: bytes,
    Decimal: float,  # TODO: support Decimal? Maybe.
}


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


class StorageTypeError(TypeError, StorageError):
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

    def set_value(instance: StorageMapping, f, k, v, vt):
        # if not isinstance(value, MINIMAL_SUPPORTED_DATATYPES):
        #     raise StorageTypeError()

        res = f(instance, k, v)
        instance.flush()
        return res

    def set_mapping(instance: StorageMapping, f, k, v):
        # It's a Mapping. Let the backend create one.
        res = f(instance, k, EMPTY_MAPPING)

        # Get the new specialized StorageMapping instance and copy each item into it
        new_mapping: StorageMapping = instance[k]
        new_mapping.update(v)
        instance.flush()
        return res

    @wraps(func)
    def wrapper_last_mapping_call_key_value(instance, key: str, value):
        # __setitem__(last_instance, key, value)
        last_instance, subkey = unfold_to_last_mapping(instance, key)

        value_type = type(value)

        # Check for lossy datatypes
        converter = LOSSY_VALUE_CONVERSION.get(value_type)
        if converter:
            # Convert value's datatype by the converter
            value = converter(value)

        if isinstance(value, Mapping):
            return set_mapping(last_instance, func, subkey, value)

        return set_value(last_instance, func, subkey, value, value_type)

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

    # Remaining datatypes which the backend can take care of by own handling.
    EXTRA_DATATYPES: tuple[Type, ...] = ()

    # def __init__(self):
    #     pass

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
        with suppress(Exception):
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

    MAPPING_CLASS = StorageMapping
    _BACKENDS: dict[str, Type["StorageBackend"]] = {}

    @classmethod
    def register_backend_class(cls, key: str, be_class: Type["StorageBackend"]):
        if key in cls._BACKENDS:
            raise RuntimeError("Key for backend already registered.")

        mapping_class = be_class.MAPPING_CLASS
        _missing = set(MINIMAL_SUPPORTED_DATATYPES) - set(mapping_class.NATIVE_DATATYPES) - set(mapping_class.EXTRA_DATATYPES)
        if _missing:
            raise NotImplementedError("Backend is missing some minimal supported datatypes: %r" % _missing)

        cls._BACKENDS[key] = be_class
        mlogger.info("Registered storage backend '%s' as %s", key, be_class)

    @classmethod
    def get_backend_class(cls, key: str) -> Type["StorageBackend"]:
        return cls._BACKENDS[key]

    @classmethod
    @abstractmethod
    def from_str_arg(cls, arg: str) -> "StorageBackend":
        """
        arg is coming directly from ENV
        """

    def __init__(self):
        self.__loaded = True

    @abstractmethod
    def get_storage_root(self) -> StorageMapping:
        pass

    @abstractmethod
    def flush(self):
        pass

    def unload(self):
        if not self.__loaded:
            return
        with suppress(Exception) as e:
            self.flush()
            self.__loaded = False

    def __repr__(self):
        return f"{self.__class__.__name__}"

    def __del__(self):
        with suppress(Exception) as e:
            self.unload()


@Serializer.register_datatype
class TestSer(Serializable):
    def __init__(self, data: bytes):
        self.data = data

    @classmethod
    def ser_from_bytes(cls, b: bytes) -> "TestSer":
        return cls(b)

    def ser_to_bytes(self) -> bytes:
        return self.data

    def __eq__(self, other):
        return isinstance(other, TestSer) and other.data == self.data


def test_mapping_values(m: StorageMapping):
    # Test minimal required types in MINIMAL_SUPPORTED_DATATYPES
    test_data = {
        "int": 4711,
        "int_neg": -42,
        "bool_True": True,
        "bool_False": False,
        "float": 1.234,
        "string_empty": "",
        "string": "a boring string",
        "string_emoji": "This robot is looking for your bugs: 🤖",
        "string_multiline": "a string on\ntwo lines",
        "string_sz": "a string containing a \x00zero char anywhere",
        "bytes": b"\x02abc\x03",
        "bytes_empty": b"",
        "nothing_to_see_here": None,
        "serializable": TestSer(b"\x00\x01TEST\xFF")
    }

    len_before = len(m)
    num_tests = len(test_data)

    for name, value in test_data.items():
        m[name] = value
        returned = m[name]
        assert returned == value, "Values not equal"
        assert type(returned) == type(value), (type(returned), type(value))

    assert len(m) == len_before + num_tests


def test_mapping(m: StorageMapping):
    test_mapping_values(m)

    len_before = len(m)

    # Missing "existing"
    assert "existing" not in m

    assert m.get("existing", "default") == "default"
    assert "existing" not in m

    assert len(m) == len_before

    # Add "existing"
    m["existing"] = "to be"
    assert "existing" in m

    assert len(m) == len_before + 1

    assert m.get("existing", "not to be") == "to be"

    # Remove "existing"
    del m["existing"]

    assert len(m) == len_before
    assert "existing" not in m


def storage_backend_test(sb: StorageBackend):
    root = sb.get_storage_root()  # type: StorageMapping

    test_mapping(root)

    root["subpath"] = {}
    subpath = root["subpath"]
    test_mapping(subpath)

    # Testing path logic
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
