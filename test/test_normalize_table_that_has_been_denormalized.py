__author__ = 'janos'

import sys
sys.path.insert(0, "../")

import unittest
import sqlalchemy as sa
import os

from sqlalchemy import INTEGER

import data_manipulation_utilities as ntd


class TestGetColumnsThatRepeatFromDatabase(unittest.TestCase):
    def setUp(self):
        if os.path.exists("test.db3"):
            os.remove("test.db3")
        self.engine = sa.create_engine("sqlite:///test.db3")
        self.connection = self.engine.connect()
        self.connection.execute("""create table denormalized_table (id integer,
  date_of_service date, dx1 varchar(8), dx2 varchar(8), dx3 varchar(8),
  cost1 float, cost2 float, cost3 float
);""")

        self.connection.execute("insert into denormalized_table (id, date_of_service, dx1, cost1) values (1, '2012-01-01','250.00', 100.50)")
        self.connection.execute("insert into denormalized_table (id, date_of_service, dx1, dx2, cost1, cost2) values (5, '2012-01-01','250.00','401.00', 120.12, 350.00)")
        self.connection.execute("insert into denormalized_table (id, date_of_service, dx1, dx2, dx3, cost1, cost2, cost3) values (11, '2012-01-01','250.00','250.01','401.00', 22.40, 34.60, 1000.50)")

        self.metadata = ntd.reflect_metadata(self.engine)

    def test_get_column_names(self):
        column_names = ntd.get_column_names_from_table("denormalized_table", self.metadata)
        self.assertEquals(["id", "date_of_service", "dx1", "dx2", "dx3", "cost1", "cost2", "cost3"], column_names)

    def test_get_column_dict(self):
        column_dict = ntd.get_column_names_with_types_dict("denormalized_table", self.metadata)

        self.assertTrue("id" in column_dict)
        self.assertEquals(INTEGER().__class__, column_dict["id"].__class__)

    def test_create_table_for_normalization(self):

        ntd.create_table_that_normalize_repeated_column("denormalized_table", "dx", "diagnosis", self.engine,
                                                identifier_column="id", mapped_identifier_column="mapped_id",
                                                sequence_field_name="sequence_id", schema=None)

    def test_create_table_and_insert(self):

        ntd.normalize_columns_that_repeat('denormalized_table', "diagnosis", "dx", self.engine, "id", "mapped_id",
                                          sequence_field_name="sequence_id")

    def test_create_table_and_insert_for_extra_columns(self):
        ntd.normalize_columns_that_repeat('denormalized_table', "diagnosis", "dx", self.engine, "id", "mapped_id",
                                          sequence_field_name="sequence_id", additional_field_list=["cost"])


class TestGetColumnsThatAppearToRepeat(unittest.TestCase):
    def setUp(self):
        self.field_names = ["id", "identifier", "DX 1", "DX 2", "DX 3", "Proc_Code_2", "Proc_Code_1"]

        self.more_difficult_field_name = ["id", "identifier", "Other Procedure  (ICD) Code 1", "Other Procedure  (ICD) Code 2", "Other Procedure  (ICD) Code 3"]

    def test_identify_columns_that_repeat(self):
        columns_that_repeat1 = ntd.get_columns_that_appear_to_repeat(self.field_names, search_pattern="DX ")
        self.assertTrue(len(columns_that_repeat1))

        first_element1 = columns_that_repeat1[0]
        self.assertEquals(tuple, first_element1.__class__)
        self.assertEquals(int, first_element1[1].__class__)

        self.assertEquals(1, first_element1[1])

        columns_that_repeat2 = ntd.get_columns_that_appear_to_repeat(self.field_names, search_pattern="Proc_Code_")
        self.assertTrue(len(columns_that_repeat2))

        first_element2 = columns_that_repeat2[0]
        self.assertEquals(tuple, first_element2.__class__)
        self.assertEquals(int, first_element2[1].__class__)

        self.assertEquals(1, first_element2[1])

    def test_more_difficult_column_names_that_repeat(self):
        columns_that_repeat = ntd.get_columns_that_appear_to_repeat(self.more_difficult_field_name, "Other Procedure  (ICD) Code ")
        self.assertTrue(len(columns_that_repeat))