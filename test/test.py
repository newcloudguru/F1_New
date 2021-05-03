import unittest
import logging

from unittest_pyspark import as_list

from main import main, get_spark_session
from unittest.mock import patch
from pyspark.sql import SparkSession
import os
import sys

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, '../src')

from utilities.etl import transform, get_joined_data


class PySparkTest(unittest.TestCase):

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
        base_path = os.path.dirname(__file__)
        cls.test_data = base_path
        cls.test_extract = cls.spark. \
            createDataFrame(
                [('Alonzo', 4.00), ('Alonzo', 5.00), ('Alonzo', 9.00)],
                ['driver', 'laptime'])
        cls.test_salary = cls.spark. \
            createDataFrame(
                [('Alonzo', 100)],
                ['driver', 'salary'])
        cls.test_expected = cls.spark. \
            createDataFrame(
                [('Alonzo', 6.00)],
                ['driver', 'avg_laptime'])

    def test_get_spark_session(self):
        spark = get_spark_session()
        expected = '''<class 'pyspark.sql.session.SparkSession'>'''
        print(str(type(spark)))

        self.assertEquals(
            str(type(spark)),
            expected)

    @patch('main.load')
    @patch('main.extract_salary')
    @patch('main.extract')
    @patch('main.get_spark_session')
    def test_main(self, mock_get_sparksession, mock_extract, mock_salary,
                  mock_load):
        mock_load.return_value = None
        mock_salary.return_value = self.test_salary
        mock_extract.return_value = self.test_extract
        mock_get_sparksession.return_value = self.spark

        main()

    @patch('main.extract')
    def testTransformation(self, mock_arrange):

        # arrange
        mock_arrange = self.test_extract
        inputDf = mock_arrange
        expectedDf = self.test_expected

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

    @patch('main.extract_salary')
    @patch('main.extract')
    def test_Salary(self, mock_extract, mock_salary):
        mock_salary = self.test_salary
        mock_extract = self.test_extract
        transformDf = transform(mock_extract)
        expected = [{"driver": "Alonzo", "avg_laptime": 6.00, "salary": 100}]
        actualDf = get_joined_data(mock_salary, transformDf)
        self.assertEqual(expected, as_list(actualDf))

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


if __name__ == '__main__':
    unittest.main()
