from abc import ABC


class Loader(ABC):
    """Base abstract class. All Loaders classes extend from this.
    """

    def load(self, ddf):
        """Base extract method, must be instantiated in extended classes.
        """
        pass
