from shared import default_configs, starter
from shared.etl import ETLService


def execute(etl_params=None):
    spark_config = default_configs.Google()
    spark = starter.start_spark(spark_config)

    service = ETLService()
    etl = service.create_etl(spark, etl_params)
    etl.execute()

    spark.stop()
