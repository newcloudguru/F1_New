import unittest
import logging
import main
from unittest.mock import patch
from pyspark.sql import SparkSession
import os
import sys
"""
# check for dependency files
if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, '../src')

from utilities.etl import transform
from utilities.spark_setup import start_spark


class PySpark1(unittest.TestCase):

    @classmethod
    def suppress_py4j_logging(cls):
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.INFO)

    @classmethod
    def create_testing_pyspark_session(cls):
        return (SparkSession.builder
                .master('local[2]')
                .appName('my-local-testing-pyspark-context')
                .enableHiveSupport()
                .getOrCreate())

    @classmethod
    def setUpClass(cls):
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()
        cls.test_df_f1_extract = cls.spark. \
            createDataFrame(
            [('Alonzo', 4.32)],
            ['driver', 'laptime'])
        cls.test_df_extract_salary = cls.spark. \
            createDataFrame(
            [('Alonzo', 100)],
            ['driver', 'salary'])

    def test_start_spark(self):
        ss, log = start_spark()
        expected = '''<class 'pyspark.sql.session.SparkSession'>'''
        self.assertEquals(
            str(type(ss)),
            expected)

    def test_transform(self):
        out = transform(self.test_df_f1_extract)

        expected = self.spark. \
            createDataFrame(
                [('Alonzo', 4.32)],
                ['driver', 'avg_laptime'])

        self.assertEquals(out.collect(), expected.collect())

    @patch('utilities.etl.transform')
    @patch('utilities.etl.extract_salary')
    @patch('utilities.etl.extract')
    @patch('utilities.spark_setup.start_spark')
    def test_main(self, mock_get_sparksession, mock_f1_extract, mock_extract_salary,
                   mock_transform):
        mock_get_sparksession.return_value = self.spark
        mock_f1_extract.return_value = self.test_df_f1_extract
        mock_extract_salary.return_value = self.test_df_extract_salary
        mock_transform.return_value = self.test_transform

        main.main()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()"""
