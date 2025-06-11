#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
"""

import datetime
import weakref
from logging import getLogger
from typing import Optional, Union, Any, Callable, Iterable, Dict, ValuesView, ItemsView, KeysView, Generator, List, \
    Tuple, Iterator, Set
from re import compile
from contextlib import suppress
from enum import EnumMeta, Enum
from collections import UserDict

logger = getLogger(__name__)

NotLoaded = object()
_valid_key = compile(r'[a-zA-Z0-9_.]+')
_invalid_chars = compile(r'[^a-zA-Z0-9_.]')


class PropertyException(Exception):
    pass


class PropertyDict(UserDict):
    """
    Holds key, value structured properties.
    Contains inherited classes of Property which wrap any data type or even sub PropertyDicts.

    Properties belonging related to root instance should only be created in modules within define_properties().
    PropertyDicts and Properties should be static, once created. Removing and extending later "should" not happen.

    Each PropertyDict may be represented by a path.
    The first created PropertyDict (root) has path=None.
    Lose PropertyDicts have their path=".".
    PropertyDicts bound in other PropertyDicts get their path fetched from containing Property.path.
    """

    __slots__ = "_parent_ref", "__weakref__", "_frozen"

    PATH_SEP = "/"

    @classmethod
    def create_keyname(cls, source: str, valid_replacement='') -> str:
        return _invalid_chars.sub(valid_replacement, source)

    def __init__(self, frozen=False, fromdict=None, /, **kwargs):
        """
        Initializes a new PropertyDict object.

        :param frozen: Can't add or remove items after initialization.
        :param kwargs: Initial properties.
        """
        self._frozen = False
        self._parent_ref: Optional[PropertyDict] = None  # Parent propertydict weakref if set

        UserDict.__init__(self, dict=fromdict, **kwargs)
        self._frozen = frozen  # TODO: Frozen

    def __del__(self):
        if self.parent is None:
            # PropertyDict is just not referenced anymore.
            self.unload()

    @property
    def parent(self) -> Optional["PropertyDict"]:
        return self._parent_ref and self._parent_ref()

    @property
    def is_root(self) -> bool:
        return self.parent is None

    @property
    def path(self) -> Optional[str]:
        """
        Returns the path representation of this PropertyDict.
        root PropertyDict returns: None
        Lose PropertyDicts are relative and return path_sep ("/" or ".")
        Others get their path from outer PropertyDict and result in dotted path representations like:
           "mainlist.sublist.propertyname"

        :return: path
        """
        if self.is_root:
            # I am root.
            # My child property's keys will be first part of path.
            return None

        elif isinstance(self.parentproperty, Property):
            # Paths are handled by Properties
            return self.parentproperty.path

        else:
            # Not part of an official path hierarchy.
            return self.path_sep

    def update(self, **new_properties: "Property"):
        existing_keys = set(self.keys())
        new_keys = set(new_properties.keys())

        conflict = new_keys & existing_keys
        if conflict:
            raise KeyError('Could not update() the PropertyDict. These keys do already exist: %s', ', '.join(conflict))

        for key, prop in new_properties.items():
            self[key] = prop

    @property
    def transaction(self) -> "LockReleaseTrigger":
        return Property.listmodel.transaction

    def get(self, path: str, default: "Property" = None) -> Optional["Property"]:
        with suppress(KeyError):
            return self[path]
        return default

    def __getitem__(self, key: str) -> "Property":
        """
        Gets a Property from PropertyDict instance by key or path.
        Paths may be provided as key. Property will be acquired recursively.

        :param key: Simple key string "myproperty" or a path "sublist.myproperty" relative to this instance.
                    Also allows leading "path_sep" to address the root propertydict: '/InputDev/0000...'
        :return: Found Property or KeyError
        """
        # if type(key) is int:
        #     key = str(key)
        #
        # elif type(key) is not str:
        #     raise TypeError("Key must be a string.")
        #

        try:
            return self._data[key]
        except KeyError:
            pass

        if self.path_sep in key:
            # Dig deeper in tree structure.
            keys = key.split(self.path_sep, maxsplit=1)

            if keys[0]:
                # Below this PropertyDict
                return self[keys[0]][keys[1]]

            # Relative to root
            return self.root()[keys[1]]

        raise KeyError(key)

    def __setitem__(self, key: str, item: Union["PropertyDict", "Property"]):
        """
        Add Property to collection.
        Key has to be unique in this list.

        :param key: String, no dots and spaces allowed.
        :param item: Property, PropertyDict which are not bound yet or any other object or value.
        """

        if type(key) is not str:
            raise TypeError('Key must be a string.')

        if not _valid_key.fullmatch(key):
            raise TypeError('Invalid chars in key. Allowed characters for keys: A-Z, a-z, 0-9, "_": ' + key)

        if key in self._data:
            raise TypeError('Key already present in PropertyDict.')

        if isinstance(item, PropertyDict):
            # Wrap PropertyDict in Property
            item = PropertyDictProperty(item, 'Nested PropertyDict (automatically wrapped)')

        if not isinstance(item, Property):
            # Not allowed. Wrap object in new simple Property
            raise ValueError('Items in PropertyDict must be Property or PropertyDict.')

        if item.parentdict is not None:
            raise TypeError('Property already assigned to another PropertyDict')

        # Link me as parent
        item._parentdict_ref = weakref.ref(self)

        # Inherit owner from my parentproperty
        owner = self.parentproperty and self.parentproperty.owner
        if owner is not None:
            item.set_owner(owner)

        # Collect new Property
        if key in self._data:
            logger.warning('Replacing existing Property with another. Key=%s, new Property: %s', key, repr(item))

        self._data[key] = item
        item._key = key

        if self._loaded:
            # Late load instantly
            logcall(item.load, errmsg='Exception on calling Property.load(): %s')

        pp = self.parentproperty
        if pp and pp.events:
            pp.events.emit(self.CHANGED)  # ToDo: context manager for event emit

    def __delitem__(self, key: str):
        # Deep deletes allowed (key = relative path)

        if self.path_sep in key:
            # Delegate deeper delete
            pd_path, key = key.rsplit(self.path_sep, 1)
            del self[pd_path][key]
            return

        delitem: Property = self._data[key]

        logcall(delitem.unload, errmsg='Exception on unloading Property: %s')
        del self._data[key]

        pp = self.parentproperty
        if pp and pp.events:
            pp.events.emit(self.CHANGED)  # ToDo: context manager for event emit

    def items(self) -> ItemsView[str, "Property"]:
        return self._data.items()

    def keys(self) -> KeysView[str]:
        return self._data.keys()

    def values(self) -> ValuesView["Property"]:
        return self._data.values()

    def paths(self) -> Generator[str, None, None]:
        path = (self.parentproperty.path if isinstance(self.parentproperty, Property) else '') or ''

        for key in self._data.keys():
            yield f'{path}{self.path_sep}{key}'

    def __repr__(self):
        loaded_status = '' if self._loaded else ' not loaded'

        if self.is_root:
            return f'<{self.__class__.__name__} ROOT ({len(self._data)} elements{loaded_status})>'

        if isinstance(self.parentproperty, Property):
            return f'<{self.__class__.__name__} {self.parentproperty.key} ({len(self._data)} elements{loaded_status})>'

        return f'<{self.__class__.__name__} ORPHAN ({len(self._data)} elements{loaded_status})>'

    def load(self):
        if self._loaded:
            return

        for prop in self._data.values():
            if isinstance(prop, Property):
                logcall(prop.load,
                        errmsg=f'Exception in PropertyDict {self.__class__!s}.load()->{prop!r}.load(): %s',
                        stack_trace=True)
        self._loaded = True

    def unload(self):
        if self._data:
            for prop in self._data.values():
                if isinstance(prop, Property):
                    logcall(prop.unload, errmsg='Exception on unloading Property: %s')

            self._data.clear()
        self._data = None

        pp = self.parentproperty
        if isinstance(pp, Property):
            pp._value = None  # remove me there
        self._parentproperty_ref = None
        self._loaded = False


class PType(Enum):
    Input = 1
    # ToDo: Input sync rw funcs
    Output = 2
    Function = 3  # Like output
    PropertyDict = 4


# Speedup enum access and use
Input = PType.Input
Output = PType.Output
Function = PType.Function


def _prop_direction(prop: "Property") -> str:
    if prop.ptype is Output or prop.ptype is Function:
        return 'OUT'

    if prop.ptype is Input:
        return 'IN'

    return ''


class Property:
    """
    Defines a simple property with an initial value of any type.
    Provides a value-property for read and write access.

    A Property may be parented to a PropertyDict.
    When parented, it is accessable by key in the PropertyDict as key-value-pair.
    Properties may contain another sub PropertyDicts.
    """

    __slots__ = '_value', '_parentdict_ref', '_path', '_id', 'desc', '_lock', '_valuepool', '_event_manager', \
                '_loaded', '_datatype', '_is_persistent', '_default_value',  '_key', '_native_datatype', '_savetime', \
                '_owner', '__weakref__', '_link', '_ptype', '_setfunc', '_getfunc', '_log', '_poll_interval', \
                '_poll_interval_min_default', '_value_time', '_in_model', '_floatprec', '_floatprec_default', 'as_human'

    _exclude_from_model = frozenset((DataType.UNDEFINED, ))  # DataType.PROPERTYDICT, DataType.ENUM
    _classlock = RLock()  # For incrementing instance counters
    _last_id = 0
    _instances_by_id: Dict[int, "Property"] = WeakValueDictionary()
    _changed_properties: List["Property"] = []
    _changed_properties_save_timeout = 5.

    listmodel: Optional[PropertiesListModel] = None  # Raw model which contains all relevant properties
    uirelevantmodel: Optional[QSortFilterProxyModel] = None  # Raw model which contains all ui relevant properties
    navigatemodel: Optional[PropertyNavigateModel] = None
    _models: Dict[str, QSortFilterProxyModel] = {}  # Filter models defined by string key like "datatype:TIME"

    # For storing meta information beyond the persistent value of the property.
    namespace_sep = ':'

    _link_namespace = 'linked_to'
    _interval_namespace = 'interval'
    _floatprec_namespace = 'float_prec'

    UPDATED = PropertyEvent('UPDATED')
    UPDATED_AND_CHANGED = PropertyEvent('UPDATED_AND_CHANGED')
    _eventids = {UPDATED, UPDATED_AND_CHANGED, PropertyDict.CHANGED}

    _worker_thread_run = False
    _worker_thread: Optional[Thread] = None
    _unresolved: List["Property"] = []
    _poll_service: List["Property"] = []  # ToDo: Deque, circular iterator etc.
    _poll_pointer = 0
    _save_pointer = 0

    _floatprec_class_default = 2  # Digits after decimal sign

    @classmethod
    def init_class(cls, parent: QObject):
        cls.listmodel = PropertiesListModel(parent)
        cls.uirelevantmodel = PropertiesByUIRelevant(cls.listmodel)
        cls.navigatemodel = PropertyNavigateModel(cls.uirelevantmodel)

    @classmethod
    def early_stop(cls):
        cls._worker_thread_run = False

        if cls._worker_thread is None:
            return

        if cls._worker_thread.is_alive():
            try:
                cls._worker_thread.join(2)
            except Exception as e:
                logger.error('Error on stopping/joining worker_thread: %s', repr(e))

        cls._worker_thread = None
        for p in tuple(cls._changed_properties):
            # immediate save
            cls._save_now(p)

    @classmethod
    def quit(cls):
        for m in cls._models.values():
            m.deleteLater()
        cls._models.clear()

        if cls.navigatemodel:
            cls.navigatemodel.deleteLater()
            cls.navigatemodel = None

        if cls.uirelevantmodel:
            cls.uirelevantmodel.deleteLater()
            cls.uirelevantmodel = None

        if cls.listmodel:
            cls.listmodel.unload()
            cls.listmodel = None

    @classmethod
    def _async_worker(cls):
        # Runs as thread
        while cls._worker_thread_run:
            sleep(0.1)

            if cls._poll_service:
                if cls._poll_pointer + 1 > len(cls._poll_service):
                    # End of list reached
                    cls._poll_pointer = 0

                p = cls._poll_service[cls._poll_pointer]
                if p._loaded:
                    try:
                        p._getfunc()
                        if p._poll_interval is None:
                            # That was a one time poll
                            del cls._poll_service[cls._poll_pointer]
                        else:
                            # Process next
                            cls._poll_pointer += 1
                    except Exception as e:
                        logger.error('Exception during calling Function from poll_service on %s: %s', repr(p), repr(e))
                        cls._poll_service.remove(p)

            if cls._changed_properties:
                if cls._save_pointer > len(cls._changed_properties) - 1:
                    # End of list reached
                    cls._save_pointer = 0

                p = cls._changed_properties[cls._save_pointer]
                if p._savetime is None or time() >= p._savetime:
                    # Save
                    cls._save_now(p)
                    del cls._changed_properties[cls._save_pointer]
                else:
                    # Check next
                    cls._save_pointer += 1

    @classmethod
    def _save_now(cls, prop: "Property"):
        with cls._classlock:
            logger.debug('Saving property %s=%s', str(prop.path), str(prop._value))
            prop.save_setting(prop._value, prop._datatype, ensure_path_absolute=False)
            prop._savetime = None

    @classmethod
    def get_by_id(cls, pr_id: int) -> Optional["Property"]:
        # Speedup accessing by id instead of paths. Usecase in http, mqtt etc.
        return cls._instances_by_id.get(pr_id)

    @classmethod
    def get_model(cls, key: str, exception=True) -> Optional[QAbstractListModel]:
        model = cls._models.get(key)
        if model is None and exception:
            raise KeyError('No model found: ' + key)
        return model

    @classmethod
    def get_datatype_model(cls, for_datatype: DataType, create=True, exception=True) \
            -> Optional[PropertiesByDataTypeModel]:
        key = 'datatype:' + for_datatype.name
        model = cls.get_model(key, exception=False)
        if model is None:
            if create:
                model = cls._models[key] = PropertiesByDataTypeModel(for_datatype, sourcemodel=cls.listmodel)
            elif exception:
                raise KeyError('No model found for datatype: ' + str(for_datatype))
        return model

    @classmethod
    def start_worker(cls):
        if cls._worker_thread_run:
            raise RuntimeError('Property worker thread already running.')

        cls._worker_thread_run = True
        cls._worker_thread = Thread(target=cls._async_worker, name='Property_worker', daemon=True)
        cls._worker_thread.start()

    @classmethod
    def create_links(cls):
        # Resolve temporary str paths in self._link to PropertyLink object
        root = PropertyDict.root()
        for inp_prop in cls._unresolved:
            if not isinstance(inp_prop._link, str):
                continue

            out_prop = root.get(inp_prop._link)
            if out_prop is None:
                # Delete broken link
                logger.warning('Linked Property to get value from does not exist. Removing link to: %s', inp_prop._link)
                inp_prop.delete_setting(cls._link_namespace, ensure_path_absolute=False)
                inp_prop._link = None
                continue

            # Create/update link
            out_prop._link_to(inp_prop)

        cls._unresolved.clear()

    def __init__(
            self,
            ptype: PType,
            datatype: DataType,
            initial_value: Any = None,
            valuepool: Optional[Dict[Any, str]] = None,
            desc: str = None,
            persistent=True,
            function_poll_min_def: Optional[Tuple[int, Optional[int]]] = None
    ):
        """
        Creates a simple Property instance.

        :param ptype: General direction of this Property in context of the module
                Input: Other Properties or UI may set the value of this Property
                Output: Only the Module sets the value of this Property
        :param datatype: DataType specification for this property
        :param initial_value: Initial value for Property. If persistent=True, this value represents the default value.
        :param valuepool: Optional iterable or dictionary containing values for this Property.
                If a dictionary is chosen, the dictkey represents the value, the dictvalue represents
                the visible text for the value.
        :param desc: Description of Property
        """
        self._lock = RLock()
        self._loaded = False
        self._ptype = ptype
        self._key: Optional[str] = None  # Cache attribute
        self._path: Optional[str] = None  # Cache attribute
        self.desc = desc
        self._parentdict_ref: Optional[Any] = None
        self._valuepool = valuepool
        self._savetime: Optional[float] = None
        self._owner: Optional[ModuleBase] = None
        self._event_manager: Optional[EventManager] = EventManager(self, self._eventids) if self._eventids else None
        self._link: Union[str, PropertyLink, None] = None
        self._log = PropLog(self)  # Log interface
        self._value_time = 0.
        self._poll_interval = None
        self._poll_interval_min_default = None
        self._in_model = False  # until actually appended
        self._datatype = datatype
        self._native_datatype = datatype_to_basic_type(datatype)
        self._default_value = None
        self._floatprec_default = self._floatprec_class_default
        self._floatprec: Optional[float] = None
        self.as_human = str

        if function_poll_min_def is not None and ptype is not Function:
            raise TypeError('function_poll_min_def is only available for Function Properties.')

        if datatype is DataType.PROPERTYDICT or isinstance(initial_value, PropertyDict) or ptype is PType.PropertyDict:
            # This Property contains a subordinal PropertyDict.
            self._is_persistent = False  # Force
            self._datatype = DataType.PROPERTYDICT  # Force
            self._ptype = PType.PropertyDict  # Force

            if initial_value is None:
                initial_value = PropertyDict()

            if not isinstance(initial_value, PropertyDict):
                raise PropertyException('If providing datatype as PROPERTYDICT you also have to provide '
                                        'a new instance of PropertyDict.')

            if initial_value.parentproperty is not None:
                raise ValueError("PropertyDict already contained by other Property.")

            self._value = initial_value  # Collect PropertyDict
            self._getfunc = self._from_cache
            self._setfunc = self._set_pd_err
            initial_value._parentproperty_ref = weakref.ref(self)

        elif self._ptype is Function:
            self._is_persistent = False
            self._value = None
            self._getfunc = partial(self._from_func, initial_value)
            self._setfunc = self._set_func_err
            self.as_human = datatype_tohuman_func.get(self._datatype, str)

            if not isinstance(function_poll_min_def, tuple) or \
                    len(function_poll_min_def) != 2 or \
                    not isinstance(function_poll_min_def[0], int) or \
                    not (function_poll_min_def[1] is None or isinstance(function_poll_min_def[1], int)):
                raise ValueError('function_poll_min_def is required for Function Properties and must contain a tuple of'
                                 ' ints: (pollinterval_min [int], pollinterval_default [int or None]).')

            # intify integers
            self._poll_interval_min_default = \
                tuple(int(interval) if interval is not None else None for interval in function_poll_min_def)

        else:
            # Any other value
            if self._ptype not in {Input, Output}:
                raise TypeError('Properties must have their PType explicitly set to Input, Output or Function.')

            self._default_value: Any = initial_value
            self._is_persistent = persistent
            self._value: Any = NotLoaded if persistent else initial_value

            if self._datatype is DataType.ENUM:
                if not isinstance(type(initial_value), EnumMeta):
                    raise ValueError('DataType.ENUM requires initial_value to be a member of an enum.')

                if valuepool is None:
                    # Create valuepool from Enum
                    enum = type(initial_value)
                    self._valuepool = {e: e.name for e in enum}

            self._getfunc = self._from_cache
            if self._ptype is Input:
                self._setfunc = self._set_value
            if self._ptype is Output:
                self._setfunc = self._set_output_err
            self.as_human = datatype_tohuman_func.get(self._datatype, str)

        with Property._classlock:
            # Unique numeric ID for fast access and easier identification
            self._id = Property._last_id = Property._last_id + 1

            # weakref collect all instances
            Property._instances_by_id[self._id] = self

    def load(self):
        if self._loaded:
            return
        self.ensure_path_absolute()  # Should be now!

        self._loaded = True

        # Load static value
        self.load_value(ensure_path_absolute=False)

        # Read the selected path or None
        self._link = None
        if self._ptype is Input:
            # Inputs can be linked to other outputs.
            # Fetch the path and resolve when all Properties are created.
            self._link = self.load_setting(None, DataType.STRING, self._link_namespace, ensure_path_absolute=False)
            if self._link:
                self._unresolved.append(self)  # Remember for later resolving
            # else:
            # Outputs are always set by the module explicitly.
            # They can be linked to other inputs

        if self._ptype is Function:
            # Read poll interval
            self.poll_interval = self.load_setting(self._poll_interval_min_default[1],
                                                   DataType.FLOAT,
                                                   namespace=self._interval_namespace,
                                                   ensure_path_absolute=False)

        if self._native_datatype is float and self._floatprec is None:
            self._floatprec = self.load_setting(self._floatprec_default,
                                                DataType.INTEGER,
                                                namespace=self._floatprec_namespace,
                                                ensure_path_absolute=False)

        # Create/update model
        if self._datatype not in self._exclude_from_model:
            self.listmodel.add_to_model(self)
            self._in_model = True

    def load_value(self, ensure_path_absolute=True):
        if ensure_path_absolute:
            self.ensure_path_absolute()

        if isinstance(self._value, PropertyDict):
            self._value.load()
            return

        if self._is_persistent and self._ptype in {Input, Output}:
            # Persistency on input and output only
            self._set_value(self.load_setting(self._default_value, self.datatype, ensure_path_absolute=False))

    @property
    def floatprec(self) -> Optional[int]:
        return self._floatprec

    @floatprec.setter
    def floatprec(self, newvalue: int):
        if self._native_datatype is not float:
            raise TypeError('floatprec is only available for float based datytypes.')

        self._floatprec = newvalue

        if newvalue == self._floatprec_default:
            # Remove setting
            self.delete_setting(self._floatprec_namespace, ensure_path_absolute=False)
        else:
            self.save_setting(newvalue, DataType.INTEGER, self._floatprec_namespace, ensure_path_absolute=False)

    @property
    def poll_interval_min(self) -> Optional[float]:
        if self._ptype is not Function:
            return None
        return self._poll_interval_min_default[0]

    @property
    def poll_interval_def(self) -> Optional[float]:
        if self._ptype is not Function:
            return None
        return self._poll_interval_min_default[1]

    @property
    def poll_interval(self) -> Optional[float]:
        return self._poll_interval

    @poll_interval.setter
    def poll_interval(self, new_interval: Optional[float]):
        if self._ptype is not Function:
            raise TypeError('poll_interval is only relevant to Property Functions.')

        if new_interval is None or new_interval <= 0.:
            # Disable interval
            self._poll_interval = None

            # But schedule for one time read at least
            if self not in self._poll_service:
                self._poll_service.append(self)

        else:
            # Enable interval
            self._poll_interval = float(new_interval)

        if self._poll_interval and self._poll_interval < self._poll_interval_min_default[0]:
            raise ValueError('Minimum poll intervall is: ' + str(self._poll_interval_min_default[0]))

        if self._poll_interval == self._poll_interval_min_default[1]:
            # If default, don't pollute settings
            self.delete_setting(self._interval_namespace, ensure_path_absolute=False)

        else:
            # Save varying interval
            self.save_setting(self._poll_interval, DataType.FLOAT, self._interval_namespace, ensure_path_absolute=False)

        if self._poll_interval:
            # Enabled
            if self not in self._poll_service:
                self._poll_service.append(self)
        else:
            # Disabled
            if self in self._poll_service:
                self._poll_service.remove(self)

    def updated_link(self):
        if self.is_linked:
            if self._ptype is Input:
                # Update my current value from linked source property
                self._setfunc(self._link.source.value)

            elif self._ptype in {Output, Function}:
                # Update my current value to linked input properties
                self._link.propagate()

        if self._in_model:
            # ToDo: Link source/destination
            if self._ptype is Input:
                self.listmodel.data_changed(self, (PropertiesListModel.IsLinked, ))
            elif self._ptype in {Output, Function}:
                self.listmodel.data_changed(self, (PropertiesListModel.IsLinked, ))

    def link_with(self, other_prop: "Property"):
        """
        Create a new persistent link from an Output Property to an Input Property
        """
        if self._ptype is PType.PropertyDict or other_prop._ptype is PType.PropertyDict:
            raise ValueError('Can\'t link PropertyDicts.')

        if self._ptype is Input:
            # We're an Input
            if other_prop._ptype not in {Output, Function}:
                raise ValueError('other_prop must be of type Output or Function.')

            source = self
            dest = other_prop
        else:
            # We're an Output/Function
            if other_prop._ptype is not Input:
                raise ValueError('other_prop must be of type Input.')

            source = other_prop
            dest = self

        # Create/update link object
        source._link_to(dest)

        # Save link permanent
        dest.save_setting(source.path, DataType.STRING, self._link_namespace, ensure_path_absolute=False)

    def unlink(self):
        if self._ptype is not Input:
            raise TypeError('Only Input Properties can be unlinked from a source.')

        if not self.is_linked:
            # Already unlinked
            return

        self._link.unlink_destination(self)

    @property
    def is_linked(self) -> bool:
        return isinstance(self._link, PropertyLink)

    def _link_to(self, dest: "Property"):
        if not self.is_linked:
            # First link on output. Create object.
            self._link = PropertyLink(self)

        # Append another link
        self._link.link_destination(dest)

    def set_owner(self, new_owner: ModuleBase):
        if self._owner is not None:
            raise RuntimeError('Owner has already been set before and is not changeable.')

        self._owner = new_owner
        if self._datatype is DataType.PROPERTYDICT and isinstance(self._value, PropertyDict):
            # Set recursively
            for subprop in self._value.values():  # type: Property
                subprop.set_owner(new_owner)

    @property
    def log(self) -> PropLog:
        return self._log

    @property
    def ptype(self) -> PType:
        return self._ptype

    @property
    def parentdict(self) -> Optional["PropertyDict"]:
        return self._parentdict_ref and self._parentdict_ref()

    @property
    def parentproperty(self) -> Optional["Property"]:
        pd = self.parentdict
        return pd and pd.parentproperty

    @property
    def owner(self) -> Optional[ModuleBase]:
        return self._owner

    @property
    def id(self) -> int:
        """Temporary numeric id of property. May change on next program run. Do not hard rely on that."""
        return self._id

    @property
    def events(self) -> Optional[EventManager]:
        return self._event_manager

    @property
    def valuepool(self) -> Optional[Dict[Any, str]]:
        return self._valuepool

    @property
    def datatype(self) -> DataType:
        return self._datatype

    @property
    def path_is_absolute(self) -> bool:
        path = self.path
        return path is not None and not path.startswith(PropertyDict.path_sep)

    def ensure_path_absolute(self, raise_exc=True) -> bool:
        if self.path_is_absolute:
            return True

        if raise_exc:
            raise ValueError("Can't read persistent settings before path has been defined. "
                             "Function has been called too early.")
        return False

    def save_setting(self, value, datatype: DataType, namespace: str = None, ensure_path_absolute=True):
        # Default implementation for saving values.
        if ensure_path_absolute:
            self.ensure_path_absolute()
        path = self.path

        if namespace is not None:
            path = f"{path}{self.namespace_sep}{namespace}"

        if datatype is DataType.ENUM:
            if type(type(value)) is not EnumMeta:
                raise TypeError('Enum settings require an enum member as value.')

            value = value.name

        settings.set(path, value, datatype)

    def load_setting(self, default, datatype: DataType, namespace: str = None, ensure_path_absolute=True):
        # Default implementation for loading values.
        if ensure_path_absolute:
            self.ensure_path_absolute()
        path = self.path

        if namespace is not None:
            path = f"{path}{self.namespace_sep}{namespace}"

        if datatype is DataType.ENUM:
            if type(type(default)) is not EnumMeta:
                raise TypeError('Enum settings require an enum member as default.')

            # Convert str to enum member
            value_str = settings.str(self.path, default.name)
            enum = type(default)
            try:
                return enum[value_str]
            except KeyError:
                logger.warning('Found unknown enum member in settings. Reverting to default for %r.', self)
                return default

        return settings.get(path, default, datatype)

    def delete_setting(self, namespace: str = None, ensure_path_absolute=True):
        # Default implementation for saving values.
        if ensure_path_absolute:
            self.ensure_path_absolute()
        path = self.path

        if namespace is not None:
            path = f"{path}{self.namespace_sep}{namespace}"

        settings.remove(path)

    def value_to_default(self):
        self.value = self._default_value

    @property
    def default_value(self) -> Any:
        return self._default_value

    @property
    def is_persistent(self) -> bool:
        return self._is_persistent

    def get_setvalue_func(self) -> Callable[[Any], None]:
        if self._ptype is not Output:
            raise ValueError('get_setvalue_func is only available for Output Properties: ' + repr(self))

        return self._set_value

    def _from_cache(self):
        # Adapter function to read from cache
        return self._value

    def _set_pd_err(self, _):
        # Exception adapter
        raise ValueError('Setting a new value on a Property containing a PropertyDict not allowed: ' + repr(self))

    def _set_func_err(self, _):
        # Exception adapter
        raise ValueError('Setting a new value on a Function Property is not allowed: ' + repr(self))

    def _set_output_err(self, _):
        # Exception adapter
        raise ValueError('Setting values to Output Properties is only ollowed by get_setvalue_func: ' + repr(self))

    def _set_value(self, newvalue):
        # Standard write to cache adapter

        if not self._loaded:
            return

        with self._lock:
            if self._native_datatype is int and type(newvalue) is float:
                # Wrong datatype. Round float to int correctly.
                newvalue = round(newvalue)

            if self._native_datatype is float and newvalue is not None:
                if type(newvalue) is int:
                    newvalue = float(newvalue)

                if isinstance(self._floatprec, int):
                    newvalue = round(newvalue, self._floatprec)

            if self._datatype is DataType.ENUM and type(newvalue) is str:
                # Convert string (from qml) back to enum.
                enum = type(self._default_value)
                newvalue = enum[newvalue]

            # Also check if value has really changed.
            changed = self._value != newvalue

            # Set new value
            self._value = newvalue

            # If linked, tell destinations too
            if self._ptype in {Output, Function} and self.is_linked:
                self._link.propagate()

            if self._event_manager:
                self._event_manager.emit(self.UPDATED)

                if changed:
                    self._event_manager.emit(self.UPDATED_AND_CHANGED)
                    if self._in_model:
                        self.listmodel.data_changed(self)

            if self._is_persistent:
                if not self.path_is_absolute:
                    logger.error('Could not save new value of Property because path is not yet defined: %r', self)
                    return

                # Schedule save
                self._savetime = time() + self._changed_properties_save_timeout
                self._changed_properties.append(self)

    def _from_func(self, func):
        t = time()
        if self._poll_interval:
            # Active polling. Check value_time.
            if t < self._value_time + self._poll_interval:
                # Value still valid. Get from cache.
                return self._value

        self._value_time = t
        res = logcall(func, errmsg='Could not execute assigned function of property: %s')
        if not isinstance(res, BaseException):
            # Cache the value
            Property._set_value(self, res)
        return self._value

    @property
    def cached_value(self) -> Any:
        return self._value

    @property
    def value(self) -> Any:
        v = logcall(self._getfunc, errmsg='Error on getting value from _getfunc() of Property: %s')
        if isinstance(v, BaseException):
            # Use cached value
            return self._value

        return v

    @value.setter
    def value(self, newvalue: Any):
        if not self._loaded:
            # May be called by a different thread a little bit later after unload() has been called.
            return

        if self._ptype is Input and self.is_linked:
            # Remove the link because explicit value was set.
            self._link.unlink_destination(self)
            self.delete_setting(self._link_namespace, ensure_path_absolute=False)

        logcall(self._setfunc, newvalue, errmsg='Error on setting value by _setfunc() of Property: %s')

    @property
    def key(self) -> Optional[str]:
        """Returns the key as string or None if not set."""
        return self._key

    @property
    def path(self) -> Optional[str]:
        """Returns the path as string or None if not set. Lose paths start with "." """

        if self._path:
            # Use valid cached path
            return self._path

        if not isinstance(self.parentdict, PropertyDict):
            # Must be in PropertyDict to calculate a valid path
            return None

        # Acquire path now.
        parentpath = self.parentdict.path

        if parentpath is None:
            # We're part of root PropertyDict
            # Our key is first part of path and will not change.
            self._path = self.key
            return self._path

        # Append our key to parentpath as full path.
        # parentpath may be lose (".", ".x.y") or absolute ("x.y").

        if parentpath == PropertyDict.path_sep:
            # lose
            newpath = f"{PropertyDict.path_sep}{self.key}"
        else:
            # lose or absolute
            newpath = f"{parentpath}{PropertyDict.path_sep}{self.key}"

        if not newpath.startswith(PropertyDict.path_sep):
            # Will not change. Cache it.
            self._path = newpath

        return newpath

    def __repr__(self):
        ret = f"<{self.__class__.__name__} {self._ptype.name} KEY='{self.key}', ID={self._id}, " \
              f"DType={self._datatype}, DEF={self._default_value}, DESC='{self.desc}'"

        if self.parentdict is not None:
            ret += f', PARENT={self.parentdict!r}'

        if self._is_persistent:
            ret += ', PERSISTENT'

        return ret + ('>' if self._loaded else ' NOT_LOADED>')

    def __contains__(self, key: str):
        return key in self.value

    def __getitem__(self, key: str):
        return self.value[key]

    def __delitem__(self, key: str):
        del self.value[key]

    def __setitem__(self, key, value):
        self.value[key] = value

    def __bool__(self):
        # Always true. Not relevant to value for safety.
        # Allows: "if self._pr_myproperty:" shortcut.
        # If not defined, __len__ is being called which may fail.
        return True

    def __len__(self):
        return len(self.value)

    def __iter__(self):
        return iter(self.value)

    def unload(self):
        if self._event_manager:
            self._event_manager.unload()
            self._event_manager = None

        if not self._loaded:
            return

        self._loaded = False

        # if self._is_persistent:
        #    logcall(self._save_now, self)

        # Dummy functions to avoid exceptions.
        self._getfunc = _null_getter
        self._setfunc = _null_setter

        if self.is_linked and self._ptype in {Output, Function}:
            self._link.unload()
            self._link = None

        if isinstance(self._value, PropertyDict):
            logcall(self._value.unload, errmsg="Exception during unloading nested PropertyDict: %s", stack_trace=True)

        if self._in_model and self.listmodel:
            # Remove from model
            self.listmodel.remove_from_model(self)
            self._in_model = False

        if self in self._poll_service:
            self._poll_service.remove(self)

        self._value = None
        del self._valuepool

        if self._lock:
            self._lock = None

        if self._id in self._instances_by_id:
            del self._instances_by_id[self._id]
        else:
            logger.warning('My id was not found in _instances_by_id for removal.')

    def __del__(self):
        if hasattr(self, '_loaded') and self._loaded:
            self.unload()
