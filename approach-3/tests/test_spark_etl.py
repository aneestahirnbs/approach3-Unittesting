
import unittest
import os, sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, DataFrameReader
parentPath = os.path.abspath("..")
if parentPath not in sys.path:
    sys.path.insert(0, parentPath)
import utility


class TestUtilityMethods(unittest.TestCase):
    def test_get_spark_context(self):
        self.assertEqual('janoo', 'janoo')
        contexts = utility.get_spark_context('local')
        self.assertEquals(len(contexts),0 )

    # def main(self):
    #     self.test_get_spark_context()
if __name__ == '__main__':
    unittest.main()