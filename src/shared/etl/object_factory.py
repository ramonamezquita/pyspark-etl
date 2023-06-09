from typing import Type, Any, Dict


class ObjectFactory:
    """Generic object factory.
    """

    def __init__(self):
        self._types = {}

    def register_type(self, name: str, type: Type[Any]) -> None:
        """Registers new type.

        Parameters
        ----------
        name : str
            String identifier.

        type : class
        """
        self._types[name] = type

    def get_types(self) -> Dict[str, Type[Any]]:
        """Returns type associated to ``name``
        """
        return self._types

    def create(self, name: str, **kwargs) -> Any:
        """Creates instance of the type associated to ``name``.

        Parameters
        ----------
        name : str
        """
        cls = self._types.get(name)
        if not cls:
            raise ValueError(name)
        return cls(**kwargs)

    @classmethod
    def from_dict(cls, d: Dict[str, Type[Any]]) -> "ObjectFactory":
        """Creates :class:`ObjectFactory` instance from dictionary.

        Parameters
        ----------
        d : dict

        Returns
        -------
        object_factory : ObjectFactory
        """

        object_factory = cls()

        for name, type in d.items():
            object_factory.register_type(name, type)

        return object_factory

    @classmethod
    def from_base(cls, base_class: Type[Any]) -> "ObjectFactory":
        """Creates :class:`ObjectFactory` instance from base class.

        The registered types are all ``base_class`` subclasses, and they are
        registered using the lowercase version of their __name__ attribute,
        that is, subcls.__name__.lower().

        Parameters
        ----------
        base_class : class
            Every derived class in ``base_class`` is registered in the
            :class:`ObjectFactory` instance.
        """

        object_factory = cls()

        for subcls in base_class.__subclasses__():
            name = subcls.__name__.lower()
            object_factory.register_type(name, subcls)

        return object_factory


class SubclassesFactory:
    """Creates factory of ``base_class``'s  subclasses.

    The registered types are all ``base_class`` subclasses, and they are
    registered using the lowercase version of their __name__ attribute,
    that is, subcls.__name__.lower().


    Parameters
    ----------
    base_class : class
        Every derived class in ``base_class`` will be available for
        creation.
    """

    def __init__(self, base_class: Type[Any]):
        self.object_factory = ObjectFactory.from_base(base_class)

    def __call__(self, name, **kwargs):
        return self.object_factory.create(name, **kwargs)

    def get_types(self):
        return self.object_factory.get_types()
