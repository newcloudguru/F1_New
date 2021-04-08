"""
unit tests: transform function 
"""
import unittest
from unittest_pyspark import as_list

from dependencies.helper import my_logger, my_timer
from dependencies.spark_setup import start_spark
from main import transform, get_joined_data
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import time
import os
import logging


class PySparkTest(unittest.TestCase):
    import logging

    def setUp(self):

        # start Spark, define path to test data
        self.spark, self.log = start_spark()
        # Get the current working directory
        directory_path = os.getcwd()
        folder_name = os.path.basename(directory_path)

        if folder_name == 'test':
            self.test_data = 'test_data/'
        else:
            self.test_data = 'test/test_data/'
        self.spark.sparkContext.setLogLevel("WARN")
        self.log.warn(f'{self.__module__} has started')

    def tearDown(self):
        # clean-up

        self.spark.stop()
        self.log.warn(f'{self.__module__} has finished')

    def testTransformation(self):
        # test data transform function.

        # arrange
        schema = StructType([
            StructField("driver", StringType(), True),
            StructField("laptime", DoubleType(), True)])

        inputDf = (
            self.spark
                .read
                .csv(self.test_data + 'arrange', header=False, schema=schema))

        expectedDf = (
            self.spark
                .read
                .csv(self.test_data + 'expected', header=True, inferSchema=True))

        expectedDf_Cols = len(expectedDf.columns)
        expectedDf_Rows = expectedDf.count()

        # act
        transformDf = transform(inputDf)

        transformDf_Cols = len(transformDf.columns)
        transformDf_Rows = transformDf.count()

        # assert
        self.assertEqual(expectedDf_Cols, transformDf_Cols)
        self.assertEqual(expectedDf_Rows, transformDf_Rows)
        self.assertTrue([col in expectedDf.columns
                         for col in transformDf.columns])

    def test_salary(self):
        data = [{"driver": "Alonzo", "salary": 500}]
        schema = StructType([
            StructField("driver", StringType(), True),
            StructField("salary", IntegerType(), True)])
        salaryDf = self.spark.createDataFrame(data, schema=schema)

        data = [{"driver": "Alonzo", "avg_laptime": 4.35}]
        schema = StructType([
            StructField("driver", StringType(), True),
            StructField("avg_laptime", DoubleType(), True)])
        driverDf = self.spark.createDataFrame(data, schema=schema)

        expected = [{"driver": "Alonzo", "avg_laptime": 4.35, "salary": 500}]

        actualDf = get_joined_data(salaryDf, driverDf)

        self.assertEqual(expected, as_list(actualDf))


if __name__ == '__main__':
    unittest.main()
