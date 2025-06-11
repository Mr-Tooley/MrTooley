# -*- coding: utf-8 -*-

"""
nmap based network scan

HOST DISCOVERY:
 -sP: Ping Scan - go no further than determining if host is online
 -PN: Treat all hosts as online -- skip host discovery


 -sS/sT/sA/sW/sM: TCP SYN/Connect()/ACK/Window/Maimon scans
 -sU: UDP Scan
PORT SPECIFICATION AND SCAN ORDER:
 -p <port ranges>: Only scan specified ports
   Ex: -p22; -p1-65535; -p U:53,111,137,T:21-25,80,139,8080

-sV: Probe open ports to determine service/version info

-O: Enable OS detection

--privileged: Assume that the user is fully privileged
--unprivileged: Assume the user lacks raw socket privileges

-n/-R: Never do DNS resolution/Always resolve [default: sometimes]

"""

import subprocess
from tempfile import TemporaryFile
from mrtooley.core.tool import Tool
from mrtooley.core.network import lookup_mac_oui
from mrtooley.core.datatypes.network import MACAddress, ip_address, IPv6Address, IPv4Address
from xml.dom.minidom import parse, Element
import platform
import re
from pathlib import Path
from typing import Optional, Union
from dataclasses import dataclass

system = platform.system()

# "192.168.51.1     0x1         0x2         d8:44:89:c9:b7:c8     *        enp5s0"
_RE_ARP_LINUX = re.compile(r"(\S+)\s+(0x1)\s+(?!0x0)(\S+)\s+(\S{2}:\S{2}:\S{2}:\S{2}:\S{2}:\S{2})")

ARP_FILE_LINUX = Path("/proc/net/arp")


@dataclass
class ScanResultStatus:
    state: bool
    reason: Optional[str]
    reason_source: Optional[str]


@dataclass
class ScanResultAddress:
    addrtype: str
    address: Union[IPv4Address, IPv6Address, MACAddress]
    vendor: Optional[str] = None


class ScanEndpoint:
    @classmethod
    def from_xmls_host(cls, node: Element, ip_to_mac: Optional[dict[str, MACAddress]] = None):
        # <status state="up" reason="arp-response" reason_ttl="0"/>
        status_nodes = node.getElementsByTagName("status")
        if status_nodes and (status_node := status_nodes[0]):
            status = ScanResultStatus(
                status_node.getAttribute("state") == "up",
                status_node.getAttribute("reason") or None,
                status_node.getAttribute("reasonsrc") or None
            )
        else:
            status = ScanResultStatus(False, None, None)

        # <address addr="192.168.51.1" addrtype="ipv4"/>
        # <address addr="D8:44:89:C9:B7:C8" addrtype="mac" vendor="TP-Link PTE."/>
        addresses: list[Union[ScanResultAddress]] = []
        address_nodes = node.getElementsByTagName("address")
        found_mac = False
        for address_node in address_nodes:
            addrtype = address_node.getAttribute("addrtype")

            addr_str = address_node.getAttribute("addr")
            if addrtype == "mac":
                found_mac = True
                addresses.append(ScanResultAddress(
                    addrtype=addrtype,
                    address=MACAddress(addr_str) if addr_str else None,
                    vendor=address_node.getAttribute("vendor") or None,
                ))

            else:  # ipv4/ipv6
                addresses.append(ScanResultAddress(
                    addrtype=addrtype,
                    address=ip_address(addr_str),
                ))

            if not found_mac and ip_to_mac:
                _found_macs = set()
                for addr in addresses:
                    if addr.addrtype in {"ipv4", "ipv6"}:
                        mac = ip_to_mac.get(addr.address.exploded)
                        if mac and mac not in _found_macs:
                            _found_macs.add(mac)
                            vendor = lookup_mac_oui(mac)
                            addresses.append(ScanResultAddress(
                                "mac",
                                mac,
                                vendor,
                            ))

        # <hostnames>
        #  <hostname name="scanme.nmap.org" type="user"/>
        #  <hostname name="li86-221.members.linode.com" type="PTR"/>
        # </hostnames>
        hostnames = []

        return cls(status, tuple(addresses), tuple(hostnames))

    def __init__(self, status: ScanResultStatus, addresses: tuple[ScanResultAddress, ...], hostnames):
        self.status = status
        self.addresses = addresses
        self.hostnames = hostnames

    def __repr__(self):
        return f"<{self.__class__.__name__} status:{self.status} addresses:{self.addresses} hostnames:{self.hostnames}>"


class NmapScanner(Tool):
    NAME = "nmap"
    DESCRIPTION = "nmap IP network scanner"
    GUID = "e949e9f5-14d9-4832-bbbf-7db72bcc42fc"

    def __init__(self):
        self.nmap_executable = "nmap"
        Tool.__init__(self)

    @classmethod
    def parse_arp(cls) -> dict[str, MACAddress]:
        return cls._parse_arp_linux()

    @classmethod
    def _parse_arp_linux(cls) -> dict[str, MACAddress]:
        result = {}
        with ARP_FILE_LINUX.open("rt") as arpfile:
            for arp_line in arpfile:
                if m := _RE_ARP_LINUX.match(arp_line):
                    ip = m.group(1)
                    result[ip] = MACAddress(m.group(4))
        return result

    @classmethod
    def _dom_to_results(cls, dom, ip_to_mac: Optional[dict[str, MACAddress]] = None):
        nmaprun = dom.getElementsByTagName("nmaprun")[0]
        hosts = nmaprun.getElementsByTagName("host")
        for host in hosts:
            yield ScanEndpoint.from_xmls_host(host, ip_to_mac)

    def pingscan(self, destination: str, lookup_missing_macs=False):
        cmd = [self.nmap_executable, "-sn", destination, "--unprivileged", "-n", "-oX", "-"]
        stderr_file = TemporaryFile("w+")

        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=stderr_file,
                                encoding="utf8",
                                text=True,
                                bufsize=1)

        try:
            dom = parse(proc.stdout)
            ip_to_mac = self.parse_arp() if lookup_missing_macs else None
            yield from self._dom_to_results(dom, ip_to_mac)

        except Exception as e:
            self.err(e)

        finally:
            proc.wait()

            stderr_file.seek(0)
            if errtext := stderr_file.read():
                self.err("STDERR: %s" % errtext)
            if proc.returncode:
                self.err("nmap exit code: " + str(proc.returncode))


t = NmapScanner()
for r in t.pingscan("192.168.51.0/24", True):
    print(r)
