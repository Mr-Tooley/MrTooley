#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import sys

from pathlib import Path

from PySide6.QtCore import QRect, QObject
from PySide6.QtGui import QIcon, QPixmap, QAction
from PySide6.QtWidgets import QApplication, QToolBar, QTreeView, QMenuBar, QMenu

from lib.windowing import BasicMainWindow
from lib.menu_controller import MenuController
from mrtooley.core.app import App

import res.res_main  # noqa: F401

SCRIPT_DIR = Path(__file__).parent


class MainWindowTreeView:
    def __init__(self, tv: QTreeView):
        self._tv = tv


class MainWindowControls:  # type: QObject
    def __init__(self, tb: QToolBar, menubar: QMenuBar):
        menu = self.menu = MenuController(menubar)

        self._play_action = tb.addAction(QIcon(QPixmap(":/icons/folder.svg")), "Play")

        m = menu.add_topmenu("Workspace", "menuWorkspace")
        self.open_workspace = m.append_action("New/Open...", "actionNewOpen")


class MainWindow(MainWindowControls, MainWindowTreeView, BasicMainWindow):  # stub: res/ui/main.ui
    def __init__(self):
        BasicMainWindow.__init__(self, "PY:/res/ui/main.ui")

        MainWindowControls.__init__(self, self.toolBar, self.menuBar())
        MainWindowTreeView.__init__(self, self.treeViewTools)

        self.app = App(self.settings)

    def closeEvent(self, event):
        BasicMainWindow.closeEvent(self, event)

    def save_settings(self):
        BasicMainWindow.save_settings(self)

    def restore_settings(self):
        BasicMainWindow.restore_settings(self)


def run_ui():
    app = QApplication(sys.argv)

    app.setOrganizationName("Digi-Solution")
    app.setOrganizationDomain("mrtooley.digi-solution.de")
    app.setApplicationName("MrTooley.UI")

    main_win = MainWindow()
    main_win.show()

    return_code = app.exec()
    sys.exit(return_code)


if __name__ == "__main__":
    run_ui()
