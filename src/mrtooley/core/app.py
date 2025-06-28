# -*- coding: utf-8 -*-

"""
Core app class
"""

from mrtooley.core.logger import module_logger
from mrtooley.core import startup_environment as se
from mrtooley.core.tool import ToolManager
from mrtooley.core.storage import StorageBackend
from mrtooley.core.workspaces import WorkspaceManager

# Import and register the sqlite backend
import mrtooley.core.storage_sqlite  # noqa


mlogger = module_logger(__name__)


class App:
    def __init__(self):
        # root storage
        # Check ENVs for special root storage settings
        storage_type = se.get("ROOT_STORAGE_TYPE", "SQLITE")
        storage_class = StorageBackend.get_backend_class(storage_type)

        storage_args = se.get("ROOT_STORAGE_ARGS") or ""
        self._base_storage = storage_class.from_str_arg(storage_args)
        rs = self._root_storage = self._base_storage.get_storage_root()

        # Tools
        config_tools = rs.get("tools")
        if not config_tools:
            # Apply a new dict
            rs["tools"] = {}
            # Return will be different
            config_tools = rs["tools"]
        self._tools = ToolManager(config_tools)

        # Workspaces
        config_workspaces = rs.get("workspaces")
        if not config_workspaces:
            # Apply a new dict
            rs["workspaces"] = {}
            # Return will be different
            config_workspaces = rs["workspaces"]
        self._workspaces = WorkspaceManager(config_workspaces)

    def run(self):
        pass


if __name__ == "__main__":
    a = App()
    a.run()
