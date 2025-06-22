# -*- coding: utf-8 -*-

"""
Core app class
"""

from pathlib import Path
from mrtooley.core.storage import StorageMapping
from mrtooley.core.workspaces import MainWorkspace, Workspace


class App:
    def __init__(self, core_settings: StorageMapping):
        self._core_settings = core_settings
        self._workspaces = []
