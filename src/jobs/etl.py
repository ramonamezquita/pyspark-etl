from shared import default_configs
from shared.etl import ETLCreator

from .base import BaseJob


class ETLJob(BaseJob):
    """ETL job.

    ETL jobs depend on a configuration file ending in "etl.json" sent to the
    spark cluster via --files option in the ``spark-submit`` command.
    For example,

    .. code-block:: bash

        spark-submit \
        --master local[*] \
        --files my-etl.json \
        entrypoint.py


    Parameters
    ----------
    job_name : str, default=None
        Job name. If None, class name is used.

    spark_config : list of tuples or DefaultConfig, default=None
        Spark config for building spark session.

    file_params : dict, default=None
        File params.
    """

    def __init__(self, job_name=None, spark_config=None, file_params=None):
        super().__init__(
            job_name=job_name,
            spark_config=spark_config,
            file_params=file_params,
            file_pattern=r"etl$",
        )

    def execute(self):
        spark = self.start_spark()
        etl_file = self.load_file()
        etl = ETLCreator(spark, etl_file).create()
        etl.execute()

        spark.stop()


class GoogleETL(ETLJob):
    def __init__(self, job_name=None, file_params=None):
        super().__init__(
            job_name=job_name,
            spark_config=default_configs.Google(),
            file_params=file_params
        )
