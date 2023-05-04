"""
Module containing helper function for use with Apache Spark.

Credits to:
https://github.com/AlexIoannides/pyspark-example-project
"""

import json
import os
from typing import List, Tuple, Optional, Dict, Union

from pyspark import SparkFiles
from pyspark.sql import SparkSession

from . import logging


class SparkStarter:
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
    """

    def __init__(
            self,
            app_name: str = 'my_spark_app',
            master: str = 'yarn',
            spark_config: Optional[List[Tuple]] = None
    ):
        self.app_name = app_name
        self.master = master

        if spark_config is None:
            spark_config = []
        self.spark_config = spark_config

    def start(self):
        """Starts Spark session, gets Spark logger and loads config files.

        Returns
        -------
        (spark, logger, config_dict) : Tuple
            A tuple of references to the Spark session, logger and config dict
            (only if available).
        """

        spark = self.create_session()
        logger = self.get_logger(spark)
        config_dict = self.get_config_dict(logger)

        return spark, logger, config_dict

    def create_session(self):
        """Creates spark session.

        Returns
        -------
        session : SparkSession
        """
        builder = (
            SparkSession
            .builder
            .appName(self.app_name)
            .master(self.master)
        )

        for key, val in self.spark_config:
            builder.config(key, val)

        return builder.getOrCreate()

    def get_logger(self, spark):
        """Creates logger.

        Parameters
        ----------
        spark: SparkSession object.
        """
        return logging.Log4j(spark)

    def get_config_dict(self, logger=None) -> Union[Dict, None]:
        """Gets config file if sent to cluster with --files.

        Returns
        -------
        config_dict : dict or None
        """
        filename = self._get_config_filename(logger)
        if filename is not None:
            with open(filename, 'r') as config_file:
                return json.load(config_file)

    def _get_config_filename(self, logger=None):
        spark_files_dir = SparkFiles.getRootDirectory()

        for filename in os.listdir(spark_files_dir):
            if filename.endswith('config.json'):
                if logger is not None:
                    logger.warn(f'Found config file: {filename}')

                return os.path.join(spark_files_dir, filename)

        if logger is not None:
            logger.warn(f'No config file found')
