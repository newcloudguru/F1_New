import os
import sys
import logging
from dependencies.etl import extract_salary, transform, extract, get_joined_data, load


# check for dependency files
if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './src')

from dependencies.spark_setup import start_spark


if __name__ == '__main__':

    # start Spark application and get Spark session, logger
    spark, log = start_spark(app='app', )
    spark.sparkContext.setLogLevel("WARN")

    # main job is starting
    log.warn('main job has started')
    logging.warning(f'{__file__} job has started')

    # execute pipeline
    df_extract = extract(spark)
    df_salary = extract_salary(spark)
    df_transform = transform(df_extract)
    df1 = get_joined_data(df_salary, df_transform)
    load(df_transform, df1)

    # cleanup
    log.warn('main job is finishing... bye!')
    logging.warning(f'{__file__} is complete ... bye :)')
    spark.stop()

