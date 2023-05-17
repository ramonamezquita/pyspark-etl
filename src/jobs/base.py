from abc import ABC, abstractmethod
from typing import Union, List, Dict, Tuple

from pyspark.sql import SparkSession
from pyspark import SparkFiles

from shared import starter, fileloaders
from shared.default_configs import DefaultConfig


class BaseJob(ABC):
    """Base class for jobs.

    .. note::
        This class should not be used directly. Use derived classes instead.

    Parameters
    ----------
    job_name : str, default=None
        Job name. If None, class name is used.

    spark_config : list of tuples or DefaultConfig, default=None
        Spark config for building spark session.

    file_pattern : str, default=None
        File name in spark root directory matching ``file_pattern`` is loaded
        when using :meth:`load_file`.

    file_params : dict, default=None
        File params.
    """

    def __init__(
            self,
            job_name: str = None,
            spark_config: Union[List[Tuple], DefaultConfig] = None,
            file_pattern: str = None,
            file_params: Dict = None
    ):
        if job_name is None:
            job_name = self.__name__

        self.job_name = job_name
        self.spark_config = spark_config
        self.file_pattern = file_pattern
        self.file_params = file_params

        self._file_loader = fileloaders.JSONFileLoader(
            root=SparkFiles.getRootDirectory())

    def start_spark(self) -> SparkSession:
        """Starts spark session using :attr:`spark_config`.

        Returns
        -------
        spark : SparkSession
        """
        return starter.start_spark(
            app_name=self.job_name, spark_config=self.spark_config)

    def load_file(self) -> Dict:
        """Loads file in spark root directory whose name matches
        :attr:`file_pattern`.

        Returns
        -------
        file : dict
            Dict
        """
        return self._file_loader.load_with_regex(
            pattern=self.file_pattern, params=self.file_params)

    @abstractmethod
    def execute(self) -> None:
        pass
