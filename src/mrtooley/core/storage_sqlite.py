# -*- coding: utf-8 -*-

"""
Sqlite3 storage backend
"""

import sqlite3
import weakref
from mrtooley.core.datatypes import Serializer
from pathlib import Path
from mrtooley.core.storage import StorageMapping, StorageBackend, StorageError, enable_deep_path_lookup
from threading import Lock
from typing import Optional, Union


class SqliteMapping(StorageMapping):
    def __init__(self, storage: "SqliteFile", root_id: Optional[int]):
        StorageMapping.__init__(self)
        self._storage = storage
        self._root_id = root_id

    @enable_deep_path_lookup
    def __getitem__(self, key: str):
        print(f"getting value for {key}...")
        if key == "root":
            return 1

        if key == "path":
            return SqliteMapping(self._storage, 1)

        if key == "sublevel" and self._root_id == 1:
            return "subvalue"

        if key == "newpath":
            return SqliteMapping(self._storage, 2)


        return "fallback value"

    @enable_deep_path_lookup
    def __setitem__(self, key: str, value):
        if isinstance(value, (StorageMapping, dict)):
            # Create a directory
            print("created dict on", key)

        print(f"setting value for {key} to {value}")


class SqliteFile(StorageBackend):
    NATIVE_DATATYPES = int, str, float, bytes, type(None)

    def __init__(self, db_file: Optional[Path] = None, create_missing=True, create_parents=False):
        StorageBackend.__init__(self)
        self._lock = Lock()

        self._connections: dict[Union[id, None], SqliteMapping] = {}

        self._db_file = db_file
        missing = db_file is None or not db_file.is_file()

        if db_file is not None:
            if not create_missing and missing:
                raise StorageError("Database file missing: %s" % db_file)

            root = db_file.parent
            if not root.is_dir():
                if not create_parents:
                    raise StorageError("Parent folders missing: %s" % root)
                root.mkdir(mode=0o700, parents=True, exist_ok=True)

        # Open/create database file
        self._connection = sqlite3.connect(":memory:" if db_file is None else db_file)
        self._connection.execute("PRAGMA foreign_keys = ON;")

        if missing:
            # Create structure
            self._create_db()

        self._path_index: dict[str, int] = {}
        self._read_path_index()

    def _create_db(self):
        with self._lock:
            with self._connection:
                self._connection.execute("""
                CREATE TABLE tree(
                id INTEGER PRIMARY KEY,
                parent INTEGER NULL,
                key TEXT NOT NULL,
                flag INTEGER NOT NULL,
                value BLOB NULL,
                FOREIGN KEY (parent) REFERENCES tree(id) ON DELETE CASCADE
                );""")
                self._connection.execute("CREATE UNIQUE INDEX parent_key ON tree(parent, key);")
                self._connection.execute("""
                CREATE TABLE flat_paths(
                path TEXT NOT NULL UNIQUE,
                parent INTEGER NOT NULL,
                FOREIGN KEY(parent) REFERENCES tree(id) ON DELETE CASCADE
                );""")

    def _read_path_index(self):
        pass

    def get_storage_root(self) -> SqliteMapping:
        return SqliteMapping(self, None)

    def flush(self):
        pass

    def unload(self):
        pass


db_path = Path("/tmp/testdb.sqlite")
if db_path.is_file():
    db_path.unlink()
db = SqliteFile(db_path)

rootstorage = db.get_storage_root()

rootstorage["root"] = 1
print(rootstorage["root"])
print(rootstorage["path/sublevel"])


rootstorage["path/sublevel2"] = 2


rootstorage["newpath/sublevel3"] = 3
print(rootstorage["newpath"])
