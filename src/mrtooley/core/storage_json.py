# -*- coding: utf-8 -*-

"""
JSON storage backend
"""

import json
from pathlib import Path
from collections.abc import Mapping
from typing import Optional, Union, Any
from mrtooley.core.logger import module_logger

from mrtooley.core import ROOTCONFIG_DIR
from mrtooley.core.datatypes import Serializable, Serializer, SerializerError
from mrtooley.core.storage import (StorageMapping, StorageBackend, make_path_aware,
                                   MINIMAL_SUPPORTED_DATATYPES, StorageError, storage_backend_test)

logger = module_logger(__name__)


class JSONMapping(StorageMapping):
    NATIVE_DATATYPES = int, str, float, type(None), bool, tuple, Mapping
    EXTRA_DATATYPES = bytes, Serializable

    _missing = set(MINIMAL_SUPPORTED_DATATYPES) - set(NATIVE_DATATYPES) - set(EXTRA_DATATYPES)
    if _missing:
        raise NotImplementedError("This backend is missing some minimal supported datatypes: %r" % _missing)

    def __init__(self, storage: "JSONFile", dict_instance: dict):
        StorageMapping.__init__(self)
        self._storage = storage
        self._dict = dict_instance

    @make_path_aware
    def __getitem__(self, key: str) -> Union[StorageMapping, Any]:
        value = self._dict[key]

        if isinstance(value, dict):
            return JSONMapping(self._storage, value)

        return value

    @make_path_aware
    def __setitem__(self, key: str, value):
        new_value_type = type(value)

        if issubclass(new_value_type, Mapping):
            # Set new unbound and raw dict
            self._dict[key] = dict(value)
        elif (new_value_type is bytes
              or issubclass(new_value_type, self.NATIVE_DATATYPES)
              or issubclass(new_value_type, Serializable)):
            self._dict[key] = value
        else:
            raise TypeError("Unsupported type: " + str(type(value)))

    @make_path_aware
    def __delitem__(self, key: str):
        del self._dict[key]

    def __len__(self):
        return len(self._dict)

    @make_path_aware
    def __contains__(self, key: str):
        return key in self._dict

    def __iter__(self):
        return iter(self._dict)

    def flush(self):
        pass  # Nothing to flush on a single mapping

    def unload(self):
        StorageMapping.unload(self)
        self._storage = None
        if hasattr(self, "_dict"):
            del self._dict

    def __repr__(self):
        return f"{self.__class__.__name__}(dict #{id(self._dict)})"


class JSONFile(StorageBackend):
    MAPPING_CLASS = JSONMapping

    @classmethod
    def from_str_arg(cls, arg: str) -> StorageBackend:
        if arg:
            file = Path(arg).expanduser()
            return cls(file, create_parents=True, create_missing=True)

        # Default
        file = ROOTCONFIG_DIR / "root_storage.json"
        return cls(file, create_parents=True, create_missing=True)

    @staticmethod
    def handle_dump_unknown(data):
        if isinstance(data, bytes):
            return {r"\BYTES": data.decode("latin1")}

        if isinstance(data, Serializable):
            return {r"\OBJECT": Serializer.pack(data).decode("latin1")}

        raise SerializerError("Cannot translate this data type to JSON string: %s" % type(data))

    @staticmethod
    def handle_object(obj: dict):
        data = obj.get(r"\BYTES")
        if isinstance(data, str):
            return data.encode("latin1")

        data = obj.get(r"\OBJECT")
        if isinstance(data, str):
            return Serializer.unpack(data.encode("latin1"))

        return obj

    def __init__(self, json_file: Optional[Path] = None, create_missing=True, create_parents=False):
        StorageBackend.__init__(self)

        self._json_file = json_file
        memory = json_file is None
        missing = memory or not json_file.is_file()

        if not create_missing and missing:
            raise StorageError("JSON file missing: %s" % json_file)

        root = json_file.parent
        if not root.is_dir():
            if not create_parents:
                raise StorageError("Parent folders missing: %s" % root)
            root.mkdir(mode=0o700, parents=True, exist_ok=True)

        # Open/create database file
        if missing or memory:
            self._dictdata = {}
        else:
            with json_file.open("rt") as fp:
                self._dictdata = json.load(fp, object_hook=self.handle_object)

    @property
    def json_file(self) -> Optional[Path]:
        return self._json_file

    def get_storage_root(self) -> JSONMapping:
        return JSONMapping(self, self._dictdata)

    def flush(self):
        if not self._json_file:
            return

        with self._json_file.open("wt") as fp:
            json.dump(self._dictdata, fp, default=self.handle_dump_unknown, indent=4)

    def __repr__(self):
        return f"{self.__class__.__name__}({self._json_file})"


StorageBackend.register_backend_class("JSON", JSONFile)


if __name__ == "__main__":
    from mrtooley.core.logger import set_log_level, DEBUG

    set_log_level(DEBUG)
    logger.debug("Starting debug")

    db_path = Path("/tmp/testdb.json")
    if db_path.is_file():
        db_path.unlink()
    db = JSONFile(db_path)

    storage_backend_test(db)
    del db
