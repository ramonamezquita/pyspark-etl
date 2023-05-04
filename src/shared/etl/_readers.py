import pandas as pd
import pyspark


class PandasCSVReader:

    def read(self, filepath, **kwargs) -> pd.DataFrame:
        return pd.read_csv(filepath, **kwargs)


class PandasParquetReader:

    def read(self, filepath, **kwargs) -> pd.DataFrame:
        return pd.read_parquet(filepath, **kwargs)


class SparkCSVReader:

    def __init__(self, spark):
        self.spark = spark

    def read(self, filepath, **kwargs) -> pyspark.sql.DataFrame:
        return self.spark.read.csv(filepath, **kwargs)


class SparkParquetReader:

    def __init__(self, spark):
        self.spark = spark

    def read(self, filepath, **kwargs) -> pyspark.sql.DataFrame:
        return self.spark.read.parquet(filepath, **kwargs)
