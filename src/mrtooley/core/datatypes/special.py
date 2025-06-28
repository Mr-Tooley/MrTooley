# -*- coding: utf-8 -*-

from mrtooley.core.datatypes import Serializable, Serializer


@Serializer.register_datatype
class ValueLink(Serializable):
    def __init__(self):
        pass

    @classmethod
    def ser_from_bytes(cls, b: bytes) -> Serializable:
        pass

    def ser_to_bytes(self) -> bytes:
        pass


@Serializer.register_datatype
class FileContentAsStr(Serializable, str):  # TODO
    def __init__(self):
        pass

    @classmethod
    def ser_from_bytes(cls, b: bytes) -> Serializable:
        pass

    def ser_to_bytes(self) -> bytes:
        pass


@Serializer.register_datatype
class FileContentAsBytes(Serializable, bytes):  # TODO
    def __init__(self):
        pass

    @classmethod
    def ser_from_bytes(cls, b: bytes) -> Serializable:
        pass

    def ser_to_bytes(self) -> bytes:
        pass

