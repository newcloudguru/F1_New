from pyspark.sql import SparkSession
from utilities import logging


def start_spark(app='spark_app', master='local[4]'):
    
    """start spark session, retrieve Spark logger
    :param app: name of spark app.
    :param master: create cluster, default local[*]
    :return: tuple (spark session, logger)
    """

    # create session and retrieve Spark logger object
    spark = SparkSession.builder.appName(app).master(master).getOrCreate()
    logger = logging.Log4j(spark)
    return spark, logger
