# -*- coding: utf-8 -*-

import logging as _logging
from typing import Optional, Type, Union

_f = "%(asctime)s %(levelname)s %(name)s.%(funcName)s@L%(lineno)d: %(message)s"

_logging.basicConfig(format=_f, level=_logging.WARNING)
_root_logger = _logging.getLogger()


_LOG_LEVEL_BY_NAME = {
    # logging._nameToLevel
    # https://docs.python.org/3/library/logging.html#logging-levels
    "NOTSET": _logging.NOTSET,
    "DEBUG": _logging.DEBUG,
    "INFO": _logging.INFO,
    "WARNING": _logging.WARNING,
    "WARN": _logging.WARNING,
    "ERROR": _logging.ERROR,
    "CRITICAL": _logging.CRITICAL,
    "FATAL": _logging.CRITICAL,
}

DEBUG = _logging.DEBUG
INFO = _logging.INFO
WARNING = _logging.WARNING
ERROR = _logging.ERROR
CRITICAL = _logging.CRITICAL


def module_logger(module_name: str) -> _logging.Logger:
    return _logging.getLogger(module_name)


def instance_logger(cls: Type, instancename: Optional[str]) -> _logging.Logger:
    if instancename is None:
        return _logging.getLogger(f"{cls.__module__}.{cls.__name__}")

    return _logging.getLogger(f"{cls.__module__}.{cls.__name__}({instancename})")


def set_log_level(loglevel: Union[str, int]):
    if isinstance(loglevel, str):
        loglevel = _LOG_LEVEL_BY_NAME[loglevel]
    _root_logger.setLevel(loglevel)
