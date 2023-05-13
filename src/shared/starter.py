"""
Module containing helper function for use with Apache Spark.

Credits to:
https://github.com/AlexIoannides/pyspark-example-project
"""

from typing import List, Tuple, Optional

from pyspark.sql import SparkSession


def start_spark(
        app_name: str = 'my_spark_app', master: str = 'yarn',
        spark_config: Optional[List[Tuple]] = None) -> SparkSession:
    """Starts a Spark session on the worker node and registers the Spark
        application with the cluster.


    Parameters
    ----------
    app_name : str
        Name of Spark app.

    master : str, default="local[*]"
        Cluster connection details.

    spark_config : list of tuple
        Key-value pairs.

    Returns
    -------
    spark : SparkSession
    """
    if spark_config is None:
        spark_config = []

    builder = (
        SparkSession
        .builder
        .appName(app_name)
        .master(master)
    )

    for key, val in spark_config:
        builder.config(key, val)

    return builder.getOrCreate()
