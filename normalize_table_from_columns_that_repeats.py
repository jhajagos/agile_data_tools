__author__ = 'jhajagos'

import data_manipulation_utilities as dmu
from optparse import OptionParser
import sqlalchemy as sa
import json
import pprint
import os


def main(table_name, new_table_name, column_name_pattern, identifier_column, identifier_column_that_maps,
         sequence_field_name, connection_url, schema=None):

    engine = sa.create_engine(connection_url)

    dmu.normalize_columns_that_repeat(table_name, new_table_name, column_name_pattern,  engine, identifier_column,
                                      identifier_column_that_maps, sequence_field_name, schema)


def set_options(options):
    options_dict = {}

    options_dict["table_name"] = options.table_name
    options_dict["new_table_name"] = options.new_table_name
    options_dict["identity_column_name"] = options.identity_column_name
    options_dict["mapped_name_identity_column"] = options.mapped_name_identity_column
    options_dict["connection_string"] = options.connection_string
    options_dict["sequence_field_name"] = options.sequence_field_name
    options_dict["schema"] = options.schema
    options_dict["column_name_pattern"] = options.column_name_pattern

    return options_dict


def load_options(json_file_name):
    with open(json_file_name, "r") as f:
        options_dict = json.load(f)
    return options_dict


def ensure_options_dict_missing_fields(options_dict={}):

    option_names = ["table_name", "new_table_name", "identity_column_name", "mapped_name_identity_column",
                    "connection_string", "header", "out_file_name", "sequence_field_name", "schema",
                    "column_name_pattern"]

    for option_name in option_names:
        if option_name not in options_dict:
            options_dict[option_name] = None

    return options_dict

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-t", "--table_name", dest="table_name", help="Table name to normalize")
    parser.add_option("-n", "--new_table_name", dest="new_table_name",
                      help="Name of destination table where data is going into")
    parser.add_option("-i", "--identity_column_name", dest="identity_column_name",
                      help="The primary key (identity column) for the table to normalize", default="id")
    parser.add_option("-m", "--mapped_name_identity_column", dest="mapped_name_identity_column",
                      help="The name that the table identity")
    parser.add_option("-c", "--connection_string", dest="connection_string", help="The SQLAlchemy connection string")
    parser.add_option("-s", "--sequence_field_name", dest="sequence_field_name",
                      help="In the destination table the name of the field of the sequence")
    parser.add_option("-e", "--schema", dest="schema", help="The name of the database schema")
    parser.add_option("-p", "--column_name_pattern", dest="column_name_pattern",
                      help="Pattern to match to fine repeated columns e.g., dx_")
    parser.add_option("-j", "--json_file_name", dest="json_file_name",
                      help="The name of the JSON file that serializes and stores or loads the options")

    (options, args) = parser.parse_args()

    if options.json_file_name:
        absolute_json_file_name = os.path.abspath(options.json_file_name)
        if os.path.exists(absolute_json_file_name):
            print("Loading options from '%s'" % absolute_json_file_name)

            with open(options.json_file_name, "r") as f:
                options_dict = json.load(f)
            pprint.pprint(options_dict)

        else:
            options_dict = set_options(options)

            with open(absolute_json_file_name, "w") as fw:
                json.dump(options_dict, fw, indent=4, separators=(',', ': '))
    else:
        options_dict = set_options({})

    options_dict = ensure_options_dict_missing_fields(options_dict)

    od = options_dict

    main(od["table_name"], od["new_table_name"], od["column_name_pattern"], od["identity_column_name"],
         od["mapped_name_identity_column"], od["sequence_field_name"], od["connection_string"], od["schema"])


