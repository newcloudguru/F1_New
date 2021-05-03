# global log
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType

# check for dependency files
if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

from utilities.helper import my_logger, my_timer
from utilities.etl import transform, get_joined_data

log = None


@my_logger
@my_timer
def extract(ss):
    """load data from csv file format.
    :param ss: spark session object.
    :return: dataframe."""

    print('In def extract')
    schema = StructType([
        StructField("driver", StringType(), True),
        StructField("laptime", DoubleType(), True)])
    df = (ss.read.csv('input', header=False, schema=schema))
    return df


@my_logger
@my_timer
def extract_salary(ss):
    """load salary data from csv file format.
    :param ss: spark session object.
    :return: dataframe.
    """
    print('In def extract salary')
    schema = StructType([
        StructField("driver", StringType(), True),
        StructField("salary", IntegerType(), True)])

    df = (ss.read.csv('input/salary', header=False, schema=schema))
    return df


@my_logger
@my_timer
def load(df, df1):
    """write output to CSV
    :param df1: joined df salary avg_laptime dataFrame
    :param df: transformed dataFrame
    :return: None
    """
    print('In def load')
    (df.coalesce(1).write.csv('output/avg_laptimes',
                              mode='overwrite', header=True))
    (df1.coalesce(1).write.csv('output/avg_laptimes_salary',
                               mode='overwrite', header=True))
    return None


def get_spark_session():
    return SparkSession.builder \
        .master("local") \
        .appName("unit testing example") \
        .getOrCreate()


def get_logger(spark):
    print('In def logger')
    global log
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)


def main():

    spark = get_spark_session()
    get_logger(spark)
    spark.sparkContext.setLogLevel("WARN")
    log.warn('main job is starting!')
    # execute pipeline
    df_extract = extract(spark)
    df_salary = extract_salary(spark)
    df_transform = transform(df_extract)
    df1 = get_joined_data(df_salary, df_transform)
    load(df_transform, df1)

    # cleanup
    log.warn('main job is finishing... bye!')
    spark.stop()


if __name__ == '__main__':
    main()
