# -*- coding: utf-8 -*-

"""
Module docstring
"""

from typing import Union, Optional
from mrtooley.core.datatypes import Serializer, Serializable
from ipaddress import ip_address, ip_network, IPv4Address, IPv6Address, IPv4Network, IPv6Network


# TODO: Serialize ip_address classes


@Serializer.register_datatype
class MACAddress(Serializable):
    """
    Represents a uniform MAC address created by various data types.
    """

    def __init__(self, mac: Union[str, bytes, "MACAddress"], lookup_vendor=False):
        if isinstance(mac, MACAddress):
            mac = mac.as_bytes

        if isinstance(mac, bytes):
            if len(mac) != 6:
                raise TypeError("MAC address in bytes requires size of 6.")

        elif isinstance(mac, str):
            macstr = mac.strip().upper().replace(":", "").replace("-", "")
            if len(macstr) != 12:
                raise TypeError("MAC address is required to consist of 12 hex characters.")
            mac = bytes([int(macstr[i:i+2], 16) for i in range(0, len(macstr), 2)])

        else:
            raise TypeError("Unknown type representing a mac address: %s" % type(mac))

        self._mac_bytes: bytes = mac

        if lookup_vendor:
            from mrtooley.core.network import lookup_mac_oui
            vendor = lookup_mac_oui(mac)
        else:
            vendor = None

        self._vendor: Optional[str] = vendor

    @property
    def as_bytes(self) -> bytes:
        return self._mac_bytes

    def as_human(self, sep=":") -> str:
        return sep.join(['%02X' % byte for byte in self._mac_bytes])

    @classmethod
    def ser_from_bytes(cls, b: bytes) -> "Serializable":
        return cls(b)

    def ser_to_bytes(self) -> bytes:
        return self._mac_bytes

    def __hash__(self):
        return hash(self._mac_bytes)

    def __bytes__(self):
        return self._mac_bytes

    def __str__(self):
        return self.as_human()

    def __eq__(self, other):
        if isinstance(other, MACAddress):
            return self._mac_bytes == other._mac_bytes

        if isinstance(other, (str, bytes)):
            return self._mac_bytes == MACAddress(other)._mac_bytes

        return NotImplemented

    def __repr__(self):
        human = self.as_human()

        if self._vendor:
            return f"{self.__class__.__name__}('{human}', <{self._vendor}>)"
        else:
            return f"{self.__class__.__name__}('{human}')"


def mac_address(address: Union[str, bytes, MACAddress]):
    if isinstance(address, MACAddress):
        return address

    return MACAddress(address)
