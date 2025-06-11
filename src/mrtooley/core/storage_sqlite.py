# -*- coding: utf-8 -*-

"""
Sqlite3 storage backend
"""

import sqlite3
import weakref
from mrtooley.core.datatypes import Serializer
from pathlib import Path
from mrtooley.core.storage import StorageMapping, StorageBackend, StorageError
from threading import Lock
from typing import Optional


class SqliteStorage(StorageMapping):
    def __init__(self, connection, root_id: int):
        StorageMapping.__init__(self)


class SqliteFile(StorageBackend):
    NATIVE_DATATYPES = int, str, float, bytes, type(None)

    def __init__(self, db_file: Optional[Path] = None, create_missing=True, create_parents=False):
        StorageBackend.__init__(self)
        self._lock = Lock()

        self._connections = []

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

    def get_storage_root(self) -> SqliteStorage:
        pass


db_path = Path("/tmp/testdb.sqlite")
db_path.unlink()
db = SqliteFile(db_path)

