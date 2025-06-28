# -*- coding: utf-8 -*-

from sys import argv
from collections.abc import Mapping
from os import environ
from pathlib import Path
from re import compile
from typing import Optional
from mrtooley.core.logger import module_logger, set_log_level

mlogger = module_logger(__name__)

RE_SYSV_ARG = compile(r"^(?:--)?([A-Za-z0-9_]+)(?:=(.*))?$")


def is_falsey(value) -> bool:
    return value in {None, False, "0", "OFF", "FALSE", "NO", "off", "false", "no"}


class _StartupEnvironment(Mapping):
    ENV_PREFIX = "MT_"

    def __init__(self):
        Mapping.__init__(self)
        prefix_length = len(self.ENV_PREFIX)

        e = self._settings = {}

        # Lowest priority
        # Environment
        for k, v in environ.items():  # type: str, Optional[str]
            if k.startswith(self.ENV_PREFIX):
                e[k[:prefix_length]] = v

        # Higher priority, may overwrite ENVs
        # Command line
        for arg in argv[1:]:
            m = RE_SYSV_ARG.match(arg)
            if m:
                key = m.group(1)
                value = m.group(2)
                e[key] = value
            else:
                mlogger.warning("Unsupported argument: '%s'", arg)

    def __len__(self):
        return len(self._settings)

    def __iter__(self):
        return iter(self._settings)

    def __getitem__(self, key: str) -> Optional[str]:
        return self._settings[key]


startup_environment = _StartupEnvironment()
loglevel = startup_environment.get("LOGLEVEL", "WARNING")
set_log_level(loglevel)


def _create_path(name: str, default_path: str) -> Path:
    p = Path(startup_environment.get(name, default_path)).expanduser()
    p.mkdir(0o600, parents=True, exist_ok=True)
    return p


ROOTCONFIG_DIR = _create_path("MT_ROOTCONFIG_DIR", "~/.mrtooley")
USER_DIR = _create_path("MT_USER_DIR", "~/MrTooley")

__all__ = "startup_environment", "ROOTCONFIG_DIR", "USER_DIR", "is_falsey"


if __name__ == "__main__":
    for k, v in startup_environment:
        print(f"{k}={v}")
