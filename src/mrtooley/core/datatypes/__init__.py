# -*- coding: utf-8 -*-

"""
Datatype base class and handling
"""

from typing import Type
from abc import ABCMeta, abstractmethod
from mrtooley.core.logger import module_logger

logger = module_logger(__name__)


class SerializerError(Exception):
    pass


class ClassNotFound(SerializerError):
    pass


class Serializable(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def ser_from_bytes(cls, b: bytes) -> "Serializable":
        pass

    @abstractmethod
    def ser_to_bytes(self) -> bytes:
        pass


class Serializer:
    _all_datatypes: dict[str, Type[Serializable]] = {}
    CLASS_DATA_SEP = b"::"

    @classmethod
    def register_datatype(cls, dt: Type[Serializable]):
        if not issubclass(dt, Serializable):
            raise TypeError("Class %r is not a subclass of Serializable" % dt)
        fullname = dt.__module__ + "." + dt.__name__
        logger.debug("Registering class %r@%d as '%s'.", dt, id(dt), fullname)

        existing = cls._all_datatypes.get(fullname)
        if existing:
            if existing == dt:
                logger.warning("Class %s already registered", fullname)
                return dt

            logger.error("A different class has already registered as: %s", fullname)
            return dt

        cls._all_datatypes[fullname] = dt
        return dt

    @classmethod
    def pack(cls, instance: Serializable) -> bytes:
        return f"{instance.__module__}.{type(instance).__name__}".encode() + cls.CLASS_DATA_SEP + instance.ser_to_bytes()

    @classmethod
    def unpack(cls, b: bytes) -> Serializable:
        parts = b.split(cls.CLASS_DATA_SEP, maxsplit=1)

        clsname = parts[0].decode()
        try:
            dt_cls = cls._all_datatypes[clsname]
        except KeyError:
            raise KeyError("Class not found or not registered: %s" % clsname) from None

        return dt_cls.ser_from_bytes(parts[1])
