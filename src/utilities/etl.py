from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType
import pyspark.sql.functions as func
from pyspark.sql.functions import round, col
from utilities.helper import my_logger, my_timer


@my_logger
@my_timer
def extract_(ss):
    """load data from csv file format.
    :param ss: spark session object.
    :return: dataframe.
    """
    print('inside extract1')
    schema = StructType([
        StructField("driver", StringType(), True),
        StructField("laptime", DoubleType(), True)])
    import time
    time.sleep(2)
    df = (ss.read.csv('input', header=False, schema=schema))
    df.show()

    print("### driver/laptimes from csv ###")
    return df


@my_logger
@my_timer
def extract_salary_(ss):
    """load salary data from csv file format.
    :param ss: spark session object.
    :return: dataframe.
    """
    print("")
    schema = StructType([
        StructField("driver", StringType(), True),
        StructField("salary", IntegerType(), True)])

    df = (ss.read.csv('input/salary', header=False, schema=schema))

    return df


@my_logger
@my_timer
def transform(df1):
    """transform extracted dataset.
    :param df: extracted dataframe.
    :return: transformed dataframe.
    """
    print('In def transform')
    df = df1.groupBy("driver") \
        .agg(func.avg("laptime")
             .alias("avg_laptime")) \
        .orderBy("avg_laptime")

    # df.select("*", round(col('avg_laptime'), 2).alias("avg")).show()

    return df


@my_logger
@my_timer
def get_joined_data(df, df1):
    print("In def get_joined_data")
    df2 = df1.join(df, on="driver", how="left")
    df2.show()
    return df2


@my_logger
@my_timer
def load_(df, df1):
    """write output to CSV
    :param df1: joined df salary avg_laptime dataFrame
    :param df: transformed dataFrame
    :return: None
    """

    (df.coalesce(1).write.csv('output/avg_laptimes',
                              mode='overwrite', header=True))
    (df1.coalesce(1).write.csv('output/avg_laptimes_salary',
                               mode='overwrite', header=True))
    return None
