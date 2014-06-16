__author__ = 'jhajagos'

import data_manipulation_utilities as dmu
from optparse import OptionParser
import sqlalchemy as sa
import json


def main(table_name, new_table_name, column_name_pattern, identifier_column, identifier_column_that_maps,
         sequence_field_name, connection_url, schema=None):

    engine = sa.create_engine(connection_url)
    dmu.normalize_columns_that_repeat(table_name, new_table_name, column_name_pattern,  engine, identifier_column,
                                      identifier_column_that_maps, sequence_field_name, schema)

def set_options(options):
    options_dict = {}
    options_dict["table_name"] = options.table_name

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-t", "--table_name", dest="table_name", help="Table name to normalize")
    parser.add_option("-n", "--new_table_name", help="Name of destination table where data is going into")
    parser.add_option("-i", "--identity_column_name", help="The primary key (identity column) for the table to normalize")
    parser.add_option("-m", "--mapped_name_identity_column", "The name that the table identity")
    parser.add_option("-e", "--engine", help="The SQLAlchemy connection string")
    parser.add_option("-s", "--sequence_field-name", help="In the destination table the name of the field of the sequence")
    parser.add_option("-c", "--schema", help="The name of the database schema")
    parser.add_option("-j", "--json_file", help="The name of the JSON file that serializes and stores or loads the options")
    #
    # parser.add_option("-c", "--connection",
    #               help="SQLAlchemy Connection String", default="sqlite:///import.db3", dest="connection_string")


