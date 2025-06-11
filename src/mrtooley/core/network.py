# -*- coding: utf-8 -*-

"""
Some network function

ENVs:
NMAP_DIR: Specifies the directory of a nmap installation. Used for nmap-mac-prefixes file.
"""

from typing import Optional, Union
import platform
import re
from threading import Lock
from pathlib import Path
from mrtooley.core.datatypes.network import MACAddress
from os import environ


system = platform.system()

NMAP_PATH_LINUX = Path("/usr/share/nmap/nmap-mac-prefixes")
NMAP_PATH_WINDOWS = Path(r"C:\Program Files (x86)\Nmap\nmap-mac-prefixes")

_lock = Lock()


# 887E25 Extreme Networks
_RE_NMAP_PREFIXES = re.compile(r"([A-F0-9]+) (.*)")

_mac6_unfold = {}  # unfold ??:??:?? to smaller registration size prefixes
_oui_cache: dict[str, str] = {}


def _check_create_cache():
    with _lock:
        if _oui_cache:  # Not empty
            return

        if nmap_dir := environ.get("NMAP_DIR"):
            nmap_path = Path(nmap_dir)
        elif system == "Linux":
            nmap_path = NMAP_PATH_LINUX
        elif system == "Windows":
            nmap_path = NMAP_PATH_WINDOWS
        elif system == "Darwin":
            raise OSError("MacOS unsupported. You can help.")

        with nmap_path.open("rt", encoding="utf8") as file:
            for line in file:
                if m := _RE_NMAP_PREFIXES.match(line):
                    mac = m.group(1)
                    name = m.group(2)

                    if len(mac) > 6:
                        # Smaller registry sub range
                        # 8C1F64 Ieee Registration Authority
                        # 8C1F64000 Suzhou Xingxiangyi Precision Manufacturing

                        mac_stem = mac[:6]

                        if mac_stem in _mac6_unfold:
                            # Mark standard mac stem prefix as a different stem size
                            if _mac6_unfold[mac_stem] != len(mac):
                                raise RuntimeError("Inconsistent mac_stem: " + mac_stem)
                        else:
                            _mac6_unfold[mac_stem] = len(mac)

                    _oui_cache[mac] = name


def lookup_mac_oui(mac: Union[bytes, str, MACAddress]) -> Optional[str]:
    # TODO: More backends
    return lookup_mac_oui_nmap(mac)


def lookup_mac_oui_nmap(mac: Union[bytes, str, MACAddress]) -> Optional[str]:
    _check_create_cache()

    mac = MACAddress(mac)

    mac6_stem = mac.as_human("")[:6]
    stemsize = _mac6_unfold.get(mac6_stem, 6)
    mac_stem = mac.as_human("")[:stemsize]

    return _oui_cache.get(mac_stem)
