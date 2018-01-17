import os
import sys
import inspect
#obj = Schema()
#utility = util.RefactorUtility(obj)

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

import unittest
from unittest.mock import MagicMock
from schema import Schema
import utility_refactor as util


class TestUtilityMethods(unittest.TestCase):



    def test_get_columns_with_table_prefix(self):
        # Arrange
        obj = Schema()

        obj.get_file_schema = MagicMock(return_value=['abc', 'xyz'])
        table_prefix = 'tbl'

        utility = util.RefactorUtility(obj)

        # Act
        actual_result = utility.get_columns_with_table_prefix(table_prefix)

        # Assert
        expectedResult = ['abc AS tbl_abc', 'xyz AS tbl_xyz']
        self.assertEqual(actual_result, expectedResult)



    def test_extract_bucket_directories(self):

        # Arrange
        path="s3a://test-bucket/outer-directory/inner-directory/another-nested-directory"
        obj = Schema()
        utility = util.RefactorUtility(obj)
        temp_bucket_name = "test-bucket"
        temp_bucket_folders = ["outer-directory", "inner-directory", "another-nested-directory"]
        temp_bucket_path = "test-bucket/outer-directory/inner-directory/another-nested-directory"
        temp_prefix = "outer-directory/inner-directory/another-nested-directory"

        # Act
        actual_result=utility.extract_bucket_directories(path)

        # Assert
        self.assertEqual(actual_result, (temp_bucket_path, temp_bucket_name, temp_bucket_folders, temp_prefix))


    def test_extract_bucket_directories_int_input(self):
        # Arrange
        path=985691
        obj = Schema()
        utility = util.RefactorUtility(obj)

        #Act & Assert
        self.assertRaises(TypeError, lambda: utility.extract_bucket_directories(path))



    def test_extract_matching_keys(self):

        #Arrange
        partition_names=["key1","key2","keyn","keym"]
        remote_keys=["key1","key2"]
        obj = Schema()
        utility = util.RefactorUtility(obj)

        # Act
        actual_result = utility.extract_matching_keys(partition_names,remote_keys)

        #Assert
        self.assertEqual(actual_result, [{'Key': 'key1'}, {'Key': 'key2'}])


if __name__ == '__main__':
    unittest.main()
