"""
Extractors classes.
"""

from abc import ABC
from typing import Protocol

from . import _readers


class Reader(Protocol):
    def read(self, filepath, **kwargs):
        ...


class Extractor(ABC):
    """Base abstract class. All Extractors classes extend from this.

    Parameters
    ----------
    reader : Reader
        Object implementing :class:`Reader` interface.
    """

    def __init__(self, reader: Reader):
        self.reader = reader

    def extract(self, filepath: str, **kwargs):
        """Base extract method, must be instantiated in extended classes.
        """
        return self.reader.read(filepath, **kwargs)


class PandasCSVExtractor(Extractor):

    def __init__(self):
        super().__init__(_readers.PandasCSVReader())


class PandasParquetExtractor(Extractor):

    def __init__(self):
        super().__init__(_readers.PandasParquetReader())


class SparkCSVExtractor(Extractor):
    """Extracts CSV files into Spark DataFrame.
    """

    def __init__(self, spark):
        super().__init__(_readers.SparkCSVReader(spark))


class SparkParquetExtractor(Extractor):
    """Extracts Parquet files into Spark DataFrame.
    """

    def __init__(self, spark):
        super().__init__(_readers.SparkParquetReader(spark))





