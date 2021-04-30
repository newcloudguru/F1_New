"""
from pyspark.sql import SparkSession
import os, sys

# check for dependency files
if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

from utilities import logging


"""def start_spark(app='spark_app', master='local[4]'):
    
    """start spark session, retrieve Spark logger
    :param app: name of spark app.
    :param master: create cluster, default local[*]
    :return: tuple (spark session, logger)
    """

    # create session and retrieve Spark logger object
    spark = SparkSession.builder.appName(app).master(master).getOrCreate()
    logger = logging.Log4j(spark)
    return spark, logger
"""

"""