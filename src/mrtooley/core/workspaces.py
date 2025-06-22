# -*- coding: utf-8 -*-

"""
Workspaces are interchangeable, cross referenceable and stackable storage spaces
"""

from mrtooley.core.storage import StorageMapping


class MainWorkspace:
    def __init__(self, workspace_config: StorageMapping):
        self._config = workspace_config


class Workspace:
    def __init__(self, name: str, storage: StorageMapping):
        self._name = name
        self._storage = storage

