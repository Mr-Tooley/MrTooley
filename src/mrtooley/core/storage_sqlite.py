# -*- coding: utf-8 -*-

"""
Sqlite3 storage backend
"""

import sqlite3
from pathlib import Path
from mrtooley.core.storage import (StorageMapping, StorageBackend, StorageError, make_path_aware,
                                   BASIC_SUPPORTED_DATATYPES, StorageKeyError, test_storage_backend)
from threading import RLock
from collections.abc import Mapping
from typing import Optional, Union, Any


FLAG_NATIVE_DATATYPE = 0
FLAG_MAPPING = 1
FLAG_BOOL = 2


class SqliteMapping(StorageMapping):
    NATIVE_DATATYPES = int, str, float, bytes, type(None)
    EXTRA_DATATYPES = bool, Mapping

    if set(NATIVE_DATATYPES) | set(EXTRA_DATATYPES) != set(BASIC_SUPPORTED_DATATYPES):
        raise NotImplementedError("Backend's supported datatypes differ from BASIC_SUPPORTED_DATATYPES")

    def __init__(self, storage: "SqliteFile", parent_id: Optional[int]):
        StorageMapping.__init__(self)
        self._storage = storage
        self._my_id = parent_id

    def sql_set(self, exid: Optional[int], key: str, flag: int, value):
        if exid is None:
            result = self._storage.safe_sql_execute(
                "INSERT INTO tree(parent, key, flag, value) VALUES(?, ?, ?, ?);",
                (self._my_id, key, flag, value)
            )
        else:
            result = self._storage.safe_sql_execute(
                "UPDATE tree SET flag=?, value=? WHERE id=?;",
                (flag, value, exid)
            )
        rc = result.rowcount
        if rc != 1:
            raise StorageError("Query did affect %d rows." % rc)

        return result

    @make_path_aware
    def __getitem__(self, key: str) -> Union[StorageMapping, Any]:
        print(f"getting value for {key}...")

        if self._my_id is None:
            qry = self._storage.safe_sql_execute(
                "SELECT id, flag, value FROM tree WHERE parent IS NULL AND key = ?;",
                (key, )
            )
        else:
            qry = self._storage.safe_sql_execute(
                "SELECT id, flag, value FROM tree WHERE parent = ? AND key = ?;",
                (self._my_id, key)
            )
        res = qry.fetchone()

        if not res:
            raise StorageKeyError(f"{key}")

        _id = res[0]
        flag = res[1]
        value = res[2]

        if flag == FLAG_NATIVE_DATATYPE:
            return value

        elif flag == FLAG_BOOL:
            return bool(value)

        elif flag == FLAG_MAPPING:
            return SqliteMapping(self._storage, _id)

        else:
            raise StorageError("Unsupported flag: " + str(flag))

    @make_path_aware
    def __setitem__(self, key: str, value):
        print(f"setting value for {key} to {value}")

        new_value_type = type(value)

        if issubclass(new_value_type, Mapping):
            new_flag = FLAG_MAPPING
        elif issubclass(new_value_type, self.NATIVE_DATATYPES):
            new_flag = FLAG_NATIVE_DATATYPE
        elif isinstance(value, bool):
            new_flag = FLAG_BOOL
        else:
            raise TypeError("Unsupported type: " + str(type(value)))

        # Check existing keyvalue
        if self._my_id is None:
            qry = self._storage.safe_sql_execute(
                "SELECT id, flag FROM tree WHERE parent IS NULL AND key = ?;",
                (key, )
            )
        else:
            qry = self._storage.safe_sql_execute(
                "SELECT id, flag FROM tree WHERE parent = ? AND key = ?;",
                (self._my_id, key)
            )

        existing = qry.fetchone()

        if existing is None:
            existing_id = None
            existing_flag = None
        else:
            existing_id = existing[0]
            existing_flag = existing[1]

        if existing_flag == FLAG_MAPPING:
            # Delete previous hierarchy
            self._storage.safe_sql_execute(
                "DELETE FROM tree WHERE id=?;",
                (existing_id, )
            )
            existing_id = None  # Doesn't exist anymore

        if new_flag == FLAG_MAPPING:
            # Just create a mapping. Don't care for its contents.
            self.sql_set(existing_id, key, new_flag, None)
        elif new_flag == FLAG_NATIVE_DATATYPE:
            self.sql_set(existing_id, key, new_flag, value)
        elif new_flag == FLAG_BOOL:
            self.sql_set(existing_id, key, new_flag, 1 if value else 0)
        else:
            raise TypeError("Unsupported flag: " + str(new_flag))

    @make_path_aware
    def __delitem__(self, key: str):
        if self._my_id is None:
            qry = self._storage.safe_sql_execute(
                "DELETE FROM tree WHERE parent IS NULL AND key=?;",
                (key,)
            )
        else:
            qry = self._storage.safe_sql_execute(
                "DELETE FROM tree WHERE parent=? AND key=?;",
                (self._my_id, key,)
            )

        if qry.rowcount == 0:
            raise StorageKeyError(f"{key}")

    def __len__(self):
        if self._my_id is None:
            qry = self._storage.safe_sql_execute(
                "SELECT COUNT(id) FROM tree WHERE parent IS NULL;",
                ()
            )
        else:
            qry = self._storage.safe_sql_execute(
                "SELECT COUNT(id) FROM tree WHERE parent=?;",
                (self._my_id,)
            )

        return qry.fetchone()[0]

    @make_path_aware
    def __contains__(self, key: str):
        if self._my_id is None:
            qry = self._storage.safe_sql_execute(
                "SELECT id FROM tree WHERE parent IS NULL AND key=?;",
                (key,)
            )
        else:
            qry = self._storage.safe_sql_execute(
                "SELECT id FROM tree WHERE parent=? AND key=?;",
                (self._my_id, key)
            )

        return qry.fetchone() is not None

    def __iter__(self):
        if self._my_id is None:
            qry = self._storage.safe_sql_execute(
                "SELECT key FROM tree WHERE parent IS NULL ORDER BY KEY;",
                ()
            )
        else:
            qry = self._storage.safe_sql_execute(
                "SELECT key FROM tree WHERE parent=? ORDER BY KEY;",
                (self._my_id,)
            )

        return iter(row[0] for row in qry)

    def flush(self):
        self._storage.flush()

    def unload(self):
        StorageMapping.unload(self)
        self._storage = None
        if hasattr(self, "_parent_id"):
            del self._parent_id

    def __repr__(self):
        if self._my_id:
            return f"{self.__class__.__name__}({self._storage.db_file} @MappingID:{self._my_id})"
        else:
            return f"{self.__class__.__name__}({self._storage.db_file})"


class SqliteFile(StorageBackend):
    MAPPING_CLASS = SqliteMapping

    def __init__(self, db_file: Optional[Path] = None, create_missing=True, create_parents=False):
        StorageBackend.__init__(self)
        self.lock = RLock()

        self._db_file = db_file
        memory = db_file is None
        missing = memory or not db_file.is_file()

        if not memory:
            if not create_missing and missing:
                raise StorageError("Database file missing: %s" % db_file)

            root = db_file.parent
            if not root.is_dir():
                if not create_parents:
                    raise StorageError("Parent folders missing: %s" % root)
                root.mkdir(mode=0o700, parents=True, exist_ok=True)

        # Open/create database file
        self._connection = sqlite3.connect(":memory:" if memory else db_file)
        self._connection.execute("PRAGMA foreign_keys = ON;")

        if missing:
            # Create structure
            self._create_db()

    @property
    def db_file(self) -> Optional[Path]:
        return self._db_file

    def safe_sql_execute(self, sql: str, params: tuple[Any, ...]) -> sqlite3.Cursor:
        with self.lock:
            cur = self._connection.cursor()
            return cur.execute(sql, params)

    def _create_db(self):
        with self.lock:
            cur = self._connection.cursor()
            cur.executescript("""
            CREATE TABLE tree(
                id INTEGER PRIMARY KEY,
                parent INTEGER NULL,
                key TEXT NOT NULL,
                flag INTEGER NOT NULL,
                value BLOB NULL,
                FOREIGN KEY (parent) REFERENCES tree(id) ON DELETE CASCADE
            );
            CREATE UNIQUE INDEX parent_key ON tree(parent, key);
            CREATE INDEX parents ON tree(parent);
            CREATE INDEX flags ON tree(flag);
            """)

    def get_storage_root(self) -> SqliteMapping:
        return SqliteMapping(self, None)

    def flush(self):
        self._connection.commit()

    def unload(self):
        SqliteFile.unload(self)
        self._connection.close()
        self._connection = None

    def __repr__(self):
        return f"{self.__class__.__name__}({self._db_file})"


def test_sqlite_storage():
    db_path = Path("/tmp/testdb.sqlite")
    if db_path.is_file():
        db_path.unlink()
    db = SqliteFile(db_path)

    test_storage_backend(db)
