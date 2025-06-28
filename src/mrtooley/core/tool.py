# -*- coding: utf-8 -*-

import importlib.metadata
from typing import Iterable, Type, Union, Optional
from enum import auto, IntEnum
from pathlib import Path
from mrtooley.core.logger import module_logger, instance_logger
from mrtooley.core.storage import StorageMapping


TOOLS_META_GROUP = "mrtooley.tools"

logger = module_logger(__name__)


class ToolCategories(IntEnum):
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


class ToolStates(IntEnum):
    Starting = auto()
    Started = auto()
    Idle = auto()
    Processing = auto()
    Error = auto()
    Stopping = auto()
    Stopped = auto()


class ToolGroup:
    NAME = "Unnamed tool group"
    TOOLS: Iterable[Type[Union["Tool", "ToolGroup"]]]


class Inputs:
    pass


class Outputs:
    pass


class Tool:
    NAME = ""
    DESCRIPTION = ""
    GUID = ""
    VERSION = 1
    CATEGORIES = ToolCategories.Unspecified
    TAGS: list[str] | str = ""
    AUTHOR = ""
    ORIGIN = ""
    """
    settings/properties
    instancename
    instancing preference: singleton, named instances
    state: stopped, starting, ready, processing, stopping, notsupported
    autorun
    input/output/trigger signals
    config version check: upgrade, downgrade
    """

    def __init__(self, instancename: Optional[str]):
        self.__instance_name: Optional[str] = instancename
        self.__settings = {}
        self.__inputs = Inputs()
        self.__ouptuts = Outputs()
        self.__state = ToolStates.Stopped
        self.__logger = instance_logger(self.__class__, instancename)

    @classmethod
    def init(cls, instance_name: Optional[str], settings: dict) -> "Tool":
        instance = cls(instance_name)
        instance.__settings = settings
        return instance

    @classmethod
    def available(cls):
        return False

    @property
    def instancename(self) -> Optional[str]:
        return self.__instance_name

    @property
    def settings(self) -> dict:
        return self.__settings

    def file_open(self, relpath: Path, mode):
        # TODO
        pass

    @property
    def inputs(self) -> Inputs:
        return self.__inputs

    @property
    def outputs(self) -> Outputs:
        return self.__ouptuts

    def info(self, text: str, *args):
        self.__logger.info(text, *args)

    def error(self, text: str, *args):
        self.__logger.error(text, *args)

    def warning(self, text: str, *args):
        self.__logger.warning(text, *args)

    def debug(self, text: str, *args):
        self.__logger.debug(text, *args)

    def exception(self, text: str, *args):
        self.__logger.exception(text, *args)

    @property
    def state(self) -> ToolStates:
        return self.__state

    @state.setter
    def state(self, new_state: ToolStates):
        self.__state = new_state


def export_function(func):
    return func


class ToolManager:
    def __init__(self, toolconfig: StorageMapping):
        self._config = toolconfig

        self._toolclasses: dict[str, Type[Tool]] = {}

        for entry_point in importlib.metadata.entry_points(group=TOOLS_META_GROUP):
            plugin_name = entry_point.name
            plugin = entry_point.load()
            self._toolclasses[plugin_name] = plugin
