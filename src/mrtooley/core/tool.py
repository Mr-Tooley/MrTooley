# -*- coding: utf-8 -*-

import importlib.metadata
from typing import Iterable, Type, Union, Optional
from enum import auto, IntEnum

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
    TAGS = ""
    """
    logging
    settings/properties
    instancename
    instancing preference: singleton, named instances
    state: stopped, starting, ready, processing, stopping, notsupported
    autorun
    input/output/trigger signals
    """

    def __init__(self, instancename: Optional[str]):
        self.__instancename: Optional[str] = instancename
        self.__settings = {}

    @classmethod
    def init(cls, instancename: Optional[str], settings: dict) -> "Tool":
        instance = cls(instancename)
        instance.__settings = settings
        return instance

    @property
    def instancename(self) -> Optional[str]:
        return self.__instancename

    @property
    def settings(self) -> dict:
        return self.__settings

    def log(self, text):
        print(text)

    def err(self, text):
        print(text)

    def warn(self, text):
        print(text)


class Controls:
    def __init__(self):
        inputs = []
        outputs = []

    def add(self, control):
        pass

    def remove(self, control):
        pass
