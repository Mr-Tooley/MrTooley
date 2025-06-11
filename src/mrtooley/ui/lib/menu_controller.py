# -*- coding: utf-8 -*-

"""
Module docstring
"""
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QMenuBar, QMenu


# stdlib imports

# Third-party imports

# Project imports

class Menu(QMenu):
    def __init__(self, title: str, name: str):
        QMenu.__init__(self, title)
        self.setObjectName(name)

    def append_submenu(self, title: str, name: str) -> "Menu":
        # m = Menu(title, name)
        pass

    def append_seperator(self, title=None):
        s = self.addSeparator()
        if title:
            s.setText(title)

    def append_action(self, title: str, name: str, shortcut=None, statustip=None, callback=None) -> QAction:
        action = QAction(title)
        action.setObjectName(name)
        if statustip:
            action.setStatusTip(statustip)
        if shortcut:
            action.setShortcut(shortcut)
        self.addAction(action)
        return action


class MenuController:
    def __init__(self, mb: QMenuBar):
        self.menubar = mb

    def add_topmenu(self, title: str, name: str) -> Menu:
        m = Menu(title, name)
        self.menubar.addMenu(m)
        return m
