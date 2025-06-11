# -*- coding: utf-8 -*-

from PySide6.QtCore import QSettings
from PySide6.QtWidgets import QMainWindow

from mrtooley.ui.lib.uifileloader import load_ui


class BasicMainWindow(QMainWindow):
    """Extends QMainWindow to offer ui loading, settings bound to the window and windows geometry persistence."""

    def __init__(self, uifile: str, settings_instance: str = None):
        QMainWindow.__init__(self)
        load_ui(uifile, self)

        self.settings = QSettings()
        self.settings.beginGroup(self.__class__.__name__ + ("" if settings_instance is None else "/" + settings_instance))
        self.restore_settings()

    def closeEvent(self, event):
        # Save settings before closing
        self.save_settings()
        QMainWindow.closeEvent(self, event)

    def save_settings(self):
        """Save window geometry and state."""
        self.settings.setValue("geometry", self.saveGeometry())
        self.settings.setValue("state", self.saveState())

    def restore_settings(self):
        """Restore window geometry and state."""
        if geometry := self.settings.value("geometry"):
            self.restoreGeometry(geometry)

        if state := self.settings.value("state"):
            self.restoreState(state)
