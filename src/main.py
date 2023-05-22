<<<<<<< Updated upstream
import argparse
import importlib
import json
import os
import sys

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

parser = argparse.ArgumentParser(description='Executes spark jobs.')
parser.add_argument('-j', '--job', type=str, required=True, help='Job name')
parser.add_argument('-c', '--default_configs-args', type=json.loads,
                    help='Config file args')
args = parser.parse_args()

job_module = importlib.import_module('jobs.%s' % args.job)
job_module.execute(args.config_args)
=======
"""Spark jobs entrypoint.
"""

import argparse
import importlib

import pyspark


def start_spark(app_name: str = 'my_spark_app') -> pyspark.sql.SparkSession:
    """Starts a Spark session.

    Parameters
    ----------
    app_name : str
        Name of Spark app.


    Returns
    -------
    spark : SparkSession
    """
    builder = (
        pyspark.sql.SparkSession
        .builder
        .appName(app_name)
    )
    return builder.getOrCreate()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run a PySpark job')

    parser.add_argument(
        '--job', type=str, required=True, dest='job_name',
        help="The name of the job module you want to run")

    parser.add_argument(
        '--job-args', nargs='*',
        help="Extra arguments to send to the PySpark job")

    args = parser.parse_args()

    # Convert job args into dictionary.
    job_args = dict(s.split('=') for s in args.job_args)

    spark = start_spark(args.job_name)
    job_module = importlib.import_module('jobs.%s' % args.job_name)
    job_module.execute(spark, **job_args)
    spark.stop()
>>>>>>> Stashed changes
