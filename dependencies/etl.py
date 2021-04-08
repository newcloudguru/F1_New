from dependencies.helper import my_logger, my_timer
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pyspark.sql.functions as func


@my_logger
@my_timer
def extract(ss):
    """load data from csv file format.
    :param ss: spark session object.
    :return: dataframe.
    """

    schema = StructType([
        StructField("driver", StringType(), True),
        StructField("laptime", DoubleType(), True)])

    df = (ss.read.csv('input', header=False, schema=schema))
    print("### driver/laptimes from csv ###")
    #df.show()
    return df


@my_logger
@my_timer
def extract_salary(ss):
    """load salary data from csv file format.
    :param ss: spark session object.
    :return: dataframe.
    """
    schema = StructType([
        StructField("driver", StringType(), True),
        StructField("salary", IntegerType(), True)])

    df = (ss.read.csv('input/salary', header=False, schema=schema))
    print("### salary from csv ###")
    #df.show()
    return df


@my_logger
@my_timer
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
    #df.show()

    return df


@my_logger
@my_timer
def get_joined_data(df, df1):
    df2 = df1.join(df, on="driver", how="left")
    print("### joined data - salary, avg-laptimes ###")
    #df2.show()
    return df2


@my_logger
@my_timer
def load(df, df1):
    """write output to CSV
    :param df1: joined df salary avg_laptime dataFrame
    :param df: transformed dataFrame
    :return: None
    """

    (df.coalesce(1).write.csv('output/avg_laptimes', mode='overwrite', header=True))
    (df1.coalesce(1).write.csv('output/avg_laptimes_salary', mode='overwrite', header=True))
    return None
