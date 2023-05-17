"""
Extractors classes.
"""

from abc import ABC, abstractmethod

from . import object_factory


class Extractor(ABC):
    """Base abstract class. All Extractors classes extend from this.

    """

    @abstractmethod
    def extract(self):
        """Base extract method, must be instantiated in extended classes.
        """
        pass


class SparkCSVExtractor(Extractor):
    """Extracts CSV files into Spark DataFrame.
    """

    def __init__(self, spark, filepath, **kwargs):
        self.spark = spark
        self.filepath = filepath
        self.kwargs = kwargs

    def extract(self):
        return self.spark.read.csv(self.filepath, **self.kwargs)


class SparkParquetExtractor(Extractor):
    """Extracts Parquet files into Spark DataFrame.
    """

    def __init__(self, spark, filepath, **kwargs):
        self.spark = spark
        self.filepath = filepath
        self.kwargs = kwargs

    def extract(self):
        return self.spark.read.parquet(self.filepath, **self.kwargs)


factory = object_factory.ObjectFactory.from_base(Extractor)

