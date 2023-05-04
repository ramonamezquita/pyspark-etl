from shared.spark_starter import SparkStarter
from shared.cloud import GoogleConfig


def execute():

    starter = SparkStarter(
        app_name='ReadAndCount',
        spark_config=GoogleConfig().pairs()
    )

    spark, log, config = starter.start()


    path_to_csv = "gs://bronze-layer/coppel/googledrive/tb_transactions/year=2023/month=4/day=6/"
    ddf = spark.read.csv(path_to_csv, header=True)
    ddf.count()

    spark.stop()





