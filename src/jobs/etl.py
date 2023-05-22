import pyspark
from .. import pyspark_etl


def execute(spark: pyspark.sql.SparkSession, **file_args):
    """ETL job.

    ETL jobs depend on a configuration file ending in "etl.json" sent to the
    spark cluster via --files option in the ``spark-submit`` command.

    For example,

    .. code-block:: bash

        spark-submit ... --files my-etl.json ...


    Parameters
    ----------
    spark : SparkSession
        SparkSession object.

    file_args : key-word args
    """
    spark_root = pyspark.SparkFiles.getRootDirectory()
    loader = pyspark_etl.fileloaders.JSONFileLoader(spark_root)
    etl_file = loader.load_with_regex(pattern=r"etl$", params=file_args)
    etl = ETLCreator(spark, etl_file).create()
    etl.execute()
