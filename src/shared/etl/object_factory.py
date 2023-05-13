from typing import Type, Any


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

    def get_type(self, name: str) -> Type[Any]:
        """Returns type associated to ``name``
        """
        return self._types.get(name)

    def create(self, name: str, **kwargs) -> Any:
        """Creates instance of the type associated to ``name``.

        Parameters
        ----------
        name : str
        """
        cls = self.get_type(name)
        if not cls:
            raise ValueError(name)
        return cls(**kwargs)

    @classmethod
    def create_from_base(cls, base_class: Type[Any]) -> "ObjectFactory":
        """Creates :class:`ObjectFactory` instance whose registered types are
        all ``base_class`` subclasses.

        Parameters
        ----------
        base_class : class
            Every derived class in ``base_class`` is registered in the
            :class:`ObjectFactory` instance.
        """

        object_factory = cls()

        for subcls in base_class.__subclasses__():
            name = subcls.__name__
            object_factory.register_type(name, subcls)

        return object_factory
