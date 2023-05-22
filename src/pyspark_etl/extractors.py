"""
Extractors classes.
"""

from abc import ABC, abstractmethod


class Extractor(ABC):
    """Base abstract class. All Extractors classes extend from this.

    """

    @abstractmethod
    def extract(self):
        """Base extract method, must be instantiated in extended classes.
        """
        pass


class CSVExtractor(Extractor):
    """Extracts CSV files into Spark DataFrame.
    """

    def __init__(self, spark, filepath, **kwargs):
        self.spark = spark
        self.filepath = filepath
        self.kwargs = kwargs

    def extract(self):
        return self.spark.read.csv(self.filepath, **self.kwargs)


class ParquetExtractor(Extractor):
    """Extracts Parquet files into Spark DataFrame.
    """

    def __init__(self, spark, filepath, **kwargs):
        self.spark = spark
        self.filepath = filepath
        self.kwargs = kwargs

    def extract(self):
        return self.spark.read.parquet(self.filepath, **self.kwargs)
