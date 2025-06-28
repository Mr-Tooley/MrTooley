# -*- coding: utf-8 -*-

"""
Workspaces are interchangeable, cross referenceable and stackable storage spaces

@/path/value
@workspacename/path/key=value

@workspacename/path/value


"""

from mrtooley.core.storage import StorageMapping


class Workspace:
    def __init__(self, name: str, storage: StorageMapping):
        self._name = name
        self._storage = storage
        self._mounts: dict[str, Workspace] = {}


class WorkspaceManager(Workspace):
    def __init__(self, workspace_config: StorageMapping):
        Workspace.__init__(self, "", workspace_config)
        self._config = workspace_config
