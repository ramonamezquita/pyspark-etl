from abc import ABC
from typing import Optional, Union, List


class Loader(ABC):
    """Base abstract class. All Loaders classes extend from this.

    Loaders load the data into the end target.
    """

    def load(self, ddf) -> None:
        """Base extract method, must be instantiated in extended classes.
        """
        pass


class ParquetLoader(Loader):

    def __init__(
            self,
            path: str,
            mode: Optional[str] = None,
            partition_by: Optional[Union[str, List[str]]] = None
    ):
        self.path = path
        self.mode = mode
        self.partition_by = partition_by

    def load(self, ddf):
        ddf.write.parquet(self.path, mode=self.mode,
                          partitionBy=self.partition_by)
