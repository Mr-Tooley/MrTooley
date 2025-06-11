# PYI created on 2025-06-02T19:41:24.266814 from 'src/mrtooley/ui/res/ui/main.ui'[mtime:1746983340.3226843]
# Suitable for 'main.py': Must contain the class MainWindow(QMainWindow)
from PySide6.QtGui import QAction
from PySide6.QtWidgets import QDockWidget, QHBoxLayout, QMainWindow, QMdiArea, QMenuBar, QStatusBar, QTextEdit, QToolBar, QTreeView, QVBoxLayout, QWidget

class MainWindow(QMainWindow):
    centralwidget: QWidget
    mdiArea: QMdiArea
    menubar: QMenuBar
    statusbar: QStatusBar
    dockTools: QDockWidget
    dockWidgetContents: QWidget
    treeViewTools: QTreeView
    toolBar: QToolBar
    dockLog: QDockWidget
    dockWidgetContents_2: QWidget
    textLog: QTextEdit
    verticalLayout: QVBoxLayout
    horizontalLayout_2: QHBoxLayout
    horizontalLayout: QHBoxLayout
    actionTools: QAction
    actionLog: QAction
