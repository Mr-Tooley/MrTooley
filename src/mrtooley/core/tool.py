# -*- coding: utf-8 -*-

import importlib.metadata
from typing import Iterable, Type, Union
from enum import Enum, auto, Flag

TOOLS_META_GROUP = "mrtooley.tools"


class Categories(Flag):
    Unspecified = auto()
    Network = auto()
    Analysis = auto()
    Management = auto()
    DeviceDriver = auto()
    ApplicationControl = auto()
    Communication = auto()
    Database = auto()
    Monitoring = auto()


def discover_tools():
    plugins = {}
    for entry_point in importlib.metadata.entry_points(group=TOOLS_META_GROUP):
        plugin_name = entry_point.name
        plugin = entry_point.load()
        plugins[plugin_name] = plugin
    return plugins


class ToolGroup:
    NAME = ""
    GUID = ""
    CATEGORIES = Categories.Unspecified

    def get_tools(self) -> Iterable[Type[Union["Tool", "ToolGroup"]]]:
        pass


class Tool:
    NAME = ""
    DESCRIPTION = ""
    GUID = ""
    VERSION = 1
    CATEGORIES = Categories.Unspecified
    """
    log
    settings
    instancename
    state
    """

    def __init__(self):
        pass

    def log(self, text):
        print(text)

    def err(self, text):
        print(text)

    def warn(self, text):
        print(text)

# --- BEGIN SIGNATURES ---
# 6c48aae5d72a8b1d
