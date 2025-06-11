# -*- coding: utf-8 -*-

"""
All about storage.


Storage locations:
- Global
- Workspaces
"""

from collections.abc import MutableMapping


class StorageError(Exception):
    pass


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


class StorageBackend:
    NATIVE_DATATYPES = ()
    PREFERRED_FALLBACK_FORMAT = bytes

    def __init__(self, path_sep="/"):
        self._path_sep = path_sep

    def get_storage_root(self) -> Storage:
        pass

    def flush(self):
        pass

    def unload(self):
        pass

