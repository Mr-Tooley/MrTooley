# -*- coding: utf-8 -*-

"""
Core app class
"""

from pathlib import Path

from PySide6.QtCore import QSettings


class App:
    def __init__(self, settings: QSettings):
        self.settings = settings
        self._current_workspace_folder = Path(str(self.settings.value("workspace", "")))

    def set_workspace(self, workspace_path: Path):


        self._current_workspace_folder = workspace_path
