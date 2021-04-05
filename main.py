import os, sys
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import pyspark.sql.functions as func
import time

# check for dependency files

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

from dependencies.spark_setup import start_spark


def timer(f):
    def wrapper():
        s = time.time()
        df = f(spark)
        d = time.time() - s
        print(f' fn took {round(d, 4)} seconds\n')
        return df

    return wrapper()


# @timer
def extract(spark):
    """load data from csv file format.
    :param spark: spark session object.
    :return: dataframe.
    """
    # s = time.time()
    schema = StructType([
        StructField("driver", StringType(), True),
        StructField("laptime", DoubleType(), True)])

    df = (spark.read.csv('input', header=False, schema=schema))
    print("### driver/laptimes from csv ###")
    df.show()
    # d = time.time() - s
    # print(f'extract fn took {round(d, 4)} seconds\n')
    return df


def extract_salary(spark):
    """load salary data from csv file format.
    :param spark: spark session object.
    :return: dataframe.
    """
    schema = StructType([
        StructField("driver", StringType(), True),
        StructField("salary", IntegerType(), True)])

    df = (spark.read.csv('input/salary', header=False, schema=schema))
    print("### salary from csv ###")
    df.show()
    # time.sleep(0.001)
    return df


def transform(df):
    """transform extracted dataset.
    :param df: extracted dataframe.
    :return: transformed dataframe.
    """

    df = df.groupBy("driver") \
        .agg(func.avg("laptime")
             .alias("avg_laptime")) \
        .orderBy("avg_laptime")

    print('### transform df ###')
    df.show()

    return df


def get_joined_data(df, df1):
    df2 = df1.join(df, on="driver", how="left")
    print("### joined data - salary, avg-laptimes ###")
    df2.show()
    return df2


def load(df, df1):
    """write output to CSV
    :param df1: joined df salary avg_laptime dataFrame
    :param df: transformed dataFrame
    :return: None
    """

    (df.coalesce(1).write.csv('output/avg_laptimes', mode='overwrite', header=True))
    (df1.coalesce(1).write.csv('output/avg_laptimes_salary', mode='overwrite', header=True))
    return None


if __name__ == '__main__':

    s = time.time()
    # start Spark application and get Spark session, logger
    spark, log = start_spark(app='app', )
    spark.sparkContext.setLogLevel("WARN")

    # main job is starting
    log.warn('main job has started')

    # execute pipeline
    df_extract = extract(spark)
    df_salary = extract_salary(spark)
    df_transform = transform(df_extract)
    df1 = get_joined_data(df_salary, df_transform)
    load(df_transform, df1)

    # cleanup
    log.warn('main job is finishing... bye!')
    spark.stop()
    d = time.time() - s
    print(f'Program took {round(d,3)} seconds to execute')
