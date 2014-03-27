__author__ = 'janos'

import sys
import os
sys.path.append(os.path.pardir)
import subprocess

from bulk_import_data_from_csv_to_db import generate_schema_from_csv_file, clean_csv_file_for_import
import unittest

class TestBulkImportData(unittest.TestCase):
    def setUp(self):
        self.file_name = "test_csv_1.csv"
        self.table_name = "test_import"
        self.db_path = "sqlite:///test.db3"

        test_db3 = os.path.join(os.path.curdir, "test.db3")
        if os.path.exists(test_db3):
            os.remove(test_db3)

    def test_loading_table(self):
        generate_schema_from_csv_file(self.file_name, self.db_path, self.table_name)

    def test_command_line_loading(self):
        pass


    def test_clean_csv(self):
        file_name = clean_csv_file_for_import(self.file_name)
        self.assertIsNotNone(file_name)