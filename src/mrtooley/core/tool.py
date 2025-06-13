# -*- coding: utf-8 -*-

import importlib.metadata
from typing import Iterable, Type, Union
from enum import auto, IntEnum

from mrtooley.core.app import App

TOOLS_META_GROUP = "mrtooley.tools"


class Categories(IntEnum):
    Unspecified = auto()
    Network = auto()
    Analysis = auto()
    Management = auto()
    DeviceDriver = auto()
    ApplicationControl = auto()
    ServiceControl = auto()
    Communication = auto()
    Database = auto()
    Monitoring = auto()
    Security = auto()
    Conversion = auto()


def discover_tools():
    plugins = {}
    for entry_point in importlib.metadata.entry_points(group=TOOLS_META_GROUP):
        plugin_name = entry_point.name
        plugin = entry_point.load()
        plugins[plugin_name] = plugin
    return plugins


class ToolGroup:
    NAME = "Unnamed tool group"
    TOOLS: Iterable[Type[Union["Tool", "ToolGroup"]]]


class Tool:
    NAME = ""
    DESCRIPTION = ""
    GUID = ""
    VERSION = 1
    CATEGORIES = Categories.Unspecified
    """
    logging
    settings/properties
    instancename
    instancing preference: singleton, named instances
    state: stopped, starting, ready, processing, stopping, notsupported
    autorun
    input/output/trigger signals
    """

    def __init__(self):
        self._app = None
        self._settings = None

    @property
    def settings(self):
        return self._settings

    def log(self, text):
        print(text)

    def err(self, text):
        print(text)

    def warn(self, text):
        print(text)

# --- BEGIN SIGNATURES ---
# 6c48aae5d72a8b1d
