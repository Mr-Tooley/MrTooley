# -*- coding: utf-8 -*-

"""
Module docstring
"""

from mrtooley.core.tool import ToolGroup, Tool
from typing import Optional


class OfficialTools(ToolGroup):
    pass


class HelloWorld(Tool):
    def __init__(self, instancename: Optional[str]):
        Tool.__init__(self, instancename)

