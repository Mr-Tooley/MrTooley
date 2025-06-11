# -*- coding: utf-8 -*-

"""
Datatype base class and handling
"""

from typing import Type
from abc import ABCMeta, abstractmethod


class SerializerError(Exception):
    pass


class ClassNotFound(SerializerError):
    pass


class Serializer:
    _all_datatypes: dict[str, Type["Serializable"]] = {}
    CLASS_DATA_SEP = b"::"

    @classmethod
    def register_datatype(cls, dt: Type["Serializable"]):
        if not issubclass(dt, Serializable):
            raise TypeError("Class %r is not a subclass of Serializable" % dt)
        cls._all_datatypes[dt.__module__ + "." + dt.__name__] = dt
        return dt

    @classmethod
    def pack(cls, instance: "Serializable") -> bytes:
        return f"{instance.__module__}.{type(instance).__name__}".encode() + cls.CLASS_DATA_SEP + instance.ser_to_bytes()

    @classmethod
    def unpack(cls, b: bytes) -> "Serializable":
        parts = b.split(cls.CLASS_DATA_SEP, maxsplit=1)
        dt_cls = cls._all_datatypes[parts[0].decode()]
        return dt_cls.ser_from_bytes(parts[1])


class Serializable(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def ser_from_bytes(cls, b: bytes) -> "Serializable":
        pass

    @abstractmethod
    def ser_to_bytes(self) -> bytes:
        pass
