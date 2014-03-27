"""
    Imports a CSV file into SQL database that is supported by SqlAlchemy.

    The purpose of this script is to make importing CSV files easy into a database
    to do quick things that can be done quickly using a relational database, like filtering and
    SQL inner joins.

    Adapted from original import_tab_delimited_to_sql.py

    Based on the engine it creates SQL for a bulk import of the data

"""

import re
from string import join
import csv
import pprint
from optparse import OptionParser
import os
import json

from sqlalchemy import Table, Column, Integer, Text, Float, String, DateTime, MetaData, create_engine


def clean_header(raw_header):
    header = []
    special_characters_map = {"#": "_POUND", "%": "_PERCENT", " ": "_", '"': "",
                              "&": "AND", "/": "_", "-": "_", ".": "_PERIOD",
                              "?": "_QUESTION", "+": "_PLUS", "(": "_", ")": "_", "$": "_DOLLAR"}

    for original_label in raw_header:  # Rewrite column names in a more SQL friendly way
        label = original_label
        for split_char in special_characters_map.keys():
            split_label = label.split(split_char)

            if len(split_label) > 1:
                label = join(split_label, special_characters_map[split_char])

        label = "_".join([x for x in label.split("_") if len(x) > 0])

        if label[-1] == "_":
            label = label[:-1]

        header.append(label)

    return header


def generate_schema_from_csv_file(file_name, connection_url, table_name="temp_table", delimiter=",", no_header=False):
    """Takes a csv file and creates a table schema for it"""
    with open(file_name, "rb") as f:

        try:
            engine = create_engine(connection_url)
        except IOError:
            print "Database could not be connected to"
            raise

        if no_header:
            with open(file_name, "rb") as ft:
                csv_reader = csv.reader(ft, delimiter=delimiter)
                row = csv_reader.next()

                header = []
                for i in range(len(row)):
                    header += ["V" + str(i)]

            csv_reader = csv.reader(f, delimiter=delimiter)

        else:
            csv_reader = csv.reader(f, delimiter=delimiter)
            raw_header = csv_reader.next()
            header = clean_header(raw_header)

        positions = {}
        data_types = {}
        field_sizes = {}

        for i in range(len(header)):
            positions[i] = header[i]
            data_types[header[i]] = {}
            field_sizes[header[i]] = 0

        # This part here will empirically determine data types from the entire file
        for row in csv_reader:
            for j in range(len(header)):
                    try:
                        data_type = get_data_type(row[j])
                        field_sizes[positions[j]] = max(field_sizes[positions[j]],len(row[j]))
                    except IndexError:
                        data_type = get_data_type("")

                    if data_types[positions[j]].has_key(data_type):
                        data_types[positions[j]][data_type] += 1
                    else:
                        data_types[positions[j]][data_type] = 1

        pprint.pprint(field_sizes)
        print
        pprint.pprint(data_types)
        f.close()

        data_type = {}
        for column_name in header:
            data_type[column_name] = find_data_type_by_precedence(data_types[column_name])

        if "ID" not in [column_name.upper() for column_name in header]:
            columns_to_create = [Column('id', Integer, primary_key=True, autoincrement=True)]
        else:
            columns_to_create = []

        for j in range(len(header)): #
            column_name = header[j]

            if data_type[column_name] is None:
                data_type[column_name] = String(1)

            if data_type[column_name] == String:
                allowed_field_sizes = [1, 4, 16, 256, 512, 1024]
                field_size = field_sizes[column_name]
                new_field_size = None
                if field_size < allowed_field_sizes[-1]:
                    for i in range(len(allowed_field_sizes)):
                        if allowed_field_sizes[i] == field_size:
                            new_field_size = allowed_field_sizes[i]
                        elif field_size < allowed_field_sizes[i] and field_size > allowed_field_sizes:
                            new_field_size = allowed_field_sizes[i]

                    if new_field_size:
                        field_size = new_field_size
                if field_size == 0:
                    field_size = 1

                field_sizes[column_name] = field_size
                data_type[column_name] = String(field_sizes[column_name])

            if data_type[column_name] == Integer:  # If the integer is too large store as string using 2**32 has 10 digits as cut off
                if field_sizes[column_name] > 9:
                    data_type[column_name] = String(field_sizes[column_name])

            columns_to_create.append(Column(column_name, data_type[column_name]))

        metadata = MetaData()
        import_table = Table(table_name, metadata, *columns_to_create)
        pprint.pprint(columns_to_create)
        metadata.create_all(engine)

        import_csv_file_using_inserts(file_name, connection_url, table_name, header, data_type, positions, delimiter)


def import_csv_file_using_inserts(file_name, connection_url, table_name, header, data_type, positions, delimiter):

    engine = create_engine(connection_url)
    connection = engine.connect()
    i = 0

    with open(file_name, "rb") as f:
        csv_reader = csv.reader(f, delimiter=delimiter)
        csv_reader.next()
        for split_line in csv_reader:

            # Handle type conversion
            data_converted = []
            columns_to_include = []
            j = 0
            for value in split_line:
                cleaned_value = clean_string(value.decode("utf8", errors="replace"))
                converted_string = convert_string(cleaned_value, data_type[positions[j]])
                if converted_string is not None:
                    columns_to_include.append(header[j])
                    data_converted.append(converted_string)
                j += 1

            # Build insert sql string
            header_string = "("
            for label in columns_to_include:
                header_string = header_string + engine.dialect.identifier_preparer.quote_identifier(label) + ","
            header_string = header_string[:-1] + ")"

            insert_template = "insert into %s  %s values (%s)" % (engine.dialect.identifier_preparer.quote_identifier(table_name), header_string,
                                                                  ("%s," * len(columns_to_include))[:-1])

            if len(data_converted) > 0:
                connection.execute(insert_template % tuple(data_converted))

            if i % 1000 == 0 and i > 0:
                print("Importing %s records" % i)
            i += 1
        print("Imported %s rows into '%s'" % (i, table_name))
    connection.close()


def find_data_type_by_precedence(data_type_hash):
    data_types = data_type_hash.keys()
    inferred_data_type = None
    for data_type in data_types:
        if data_type is not None:
            if inferred_data_type is None and data_type is not None: #initially we assume that the data type is not None
                inferred_data_type = data_type
            else:
                if inferred_data_type == Integer and data_type == Float:
                    inferred_data_type = Float
                elif data_type == String:
                    inferred_data_type = String
                    
    if inferred_data_type is None:
        return Integer
    else:
        return inferred_data_type


def find_most_common_data_type(data_type_hash):
    data_types = data_type_hash.keys()
    pprint.pprint(data_types)
    exit()
    dt_max = (None, 0)
    for dt in data_types:
        if dt is not None:
            if data_type_hash[dt] > dt_max[1]:
                dt_max = (dt,data_type_hash[dt])
    if String in data_type_hash:
        return String
    else:
        return dt_max[0]


def clean_csv_file_for_import(csv_file_name, delimiter=",", header = True):
    "Cleans a CSV file and returns"

    abs_csv_file_for_import = os.path.abspath(csv_file_name)
    base_path, pure_csv_file_name = os.path.split(abs_csv_file_for_import)

    pure_csv_base_name,extension = os.path.splitext(pure_csv_file_name)

    cleaned_csv_file_name = pure_csv_base_name + "_cleaned.csv"

    abs_cleaned_csv_file_name = os.path.join(base_path, cleaned_csv_file_name)
    print(abs_cleaned_csv_file_name)

    with open(abs_csv_file_for_import, "rb") as f:
        with open(abs_cleaned_csv_file_name, "wb") as fw:

            csv_reader = csv.reader(f, delimiter=delimiter)
            csv_writer = csv.writer(fw)

            i = 0

            if header:
                header = csv_reader.next()
                header_cleaned = clean_header(header)
                csv_writer.writerow(header_cleaned)

            for row in csv_reader:
                cleaned_row = [clean_string(item) for item in row]
                csv_writer.writerow(cleaned_row)

                i += 1
    return abs_cleaned_csv_file_name

def clean_string(string_to_clean):
    """Cleans a string for importing into a sql database"""
    # Right now we only preprocess money string
    re_money =  re.compile(r"\$[0-9,.]+")
    re_float = re.compile(r"-?([0-9+]*\.?|[eE]?|[0-9]?)+$")
    re_quotes = re.compile(r'^".*"$')
    re_us_date_format = re.compile(r"[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}$")
    
    string_to_clean = string_to_clean.rstrip()
   
    if re_quotes.match(string_to_clean):
        string_to_clean = string_to_clean[1:-1]
    
    if re_money.match(string_to_clean):
        string_to_clean = join(string_to_clean.split(","),"")[1:]
        if "." not in string_to_clean:  # if there is no decimal add one so it is imported as a float
            string_to_clean = string_to_clean + ".00"
   
    if re_float.match(string_to_clean):
       string_to_clean = join(string_to_clean.split(","),"")
       
    if re_us_date_format.match(string_to_clean):
        date_split = string_to_clean.split("/")
        
        month = int(date_split[0])
        day = int(date_split[1])
        year_string = date_split[2]
        
        year = int(year_string)
        
        if len(year_string) == 1:
            year_string = "0" + year_string
        
        date_string = ""
        
        if len(year_string) < 4:
            if year < 100:
                if year > 50:
                    year_string = "19%s" % year_string
                else:
                    year_string = "20%s" % year_string
        else:
            year_string = "%s" % year
        
        date_string = "%s-%s-%s" % (year_string, month, day)
        string_to_clean = date_string    
        
    return string_to_clean


def convert_string(string_to_convert,data_type):
    if "'" in string_to_convert:
        string_to_convert = join(string_to_convert.split("'"),"''")
    
    if "%" in string_to_convert:
        string_to_convert = join(string_to_convert.split("%"),"%%")
        
    if string_to_convert == "":
        return "NULL"
    elif data_type == Float:
        return float(string_to_convert)
    elif data_type == Integer:
        return int(string_to_convert)
    else:
        return "'%s'" % string_to_convert
    
    
def get_data_type(string_to_evaluate):
    """Take a string and returns a SQLAlchemy data type class"""
    re_integer = re.compile(r"[0-9]+$")
    re_float = re.compile(r"([0-9]+\.[0-9]+[eE](\+|\-)?[0-9]+|[0-9]+[eE](\+|\-)?[0-9]+|[0-9]+\.[0-9]*|\.[0-9]+[eE](\+|\-)?[0-9]+|\.[0-9]+|[0-9]+)$")                           
    re_odbc_date = re.compile(r"[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$")
    re_date = re.compile(r"[0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4}")
    if string_to_evaluate == "":
        return None
    elif re_integer.match(string_to_evaluate):
        return Integer
    elif re_odbc_date.match(string_to_evaluate):
        return DateTime
    elif re_float.match(string_to_evaluate):
        return Float
    elif re_date.match(string_to_evaluate):
        return DateTime
    else:
        return String
    

if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-f", "--file", dest="file_name",
                      help="CSV file to import")

    parser.add_option("-d", "--delimiter", dest="delimiter",
                      help="default delimiter is ','", default=",")

    parser.add_option("-c", "--connection",
                      help="SQLAlchemy Connection String", default="sqlite:///import.db3", dest="connection_string")

    parser.add_option("-t", "--tablename",
                      help="SQLALchemy connection string", default="csv_import_table", dest="table_name")

    parser.add_option("-n", "--noheader",
                      help="Whether there is a header present or not", default=False, dest="no_headers")

    parser.add_option("-j", "--jsonfile", default=False, dest="json_file_name")

    (options, args) = parser.parse_args()

    if options.json_file_name:
        absolute_json_file_name = os.path.abspath(options.json_file_name)
        if os.path.exists(absolute_json_file_name):
            print("Loading options from '%s'" % absolute_json_file_name)

            with open(options.json_file_name, "r") as f:
                options_dict = json.load(f)
            pprint.pprint(options_dict)

        else:
            options_dict = {}
            options_dict["file_name"] = options.file_name
            options_dict["connection_string"] = options.connection_string
            options_dict["table_name"] = options.table_name
            options_dict["delimiter"] = options.delimiter
            options_dict["no_headers"] = options.no_headers

            with open(absolute_json_file_name, "w") as fw:
                json.dump(options_dict, fw, indent=4, separators=(',', ': '))
    else:
        options_dict = {}
        options_dict["file_name"] = options.file_name
        options_dict["connection_string"] = options.connection_string
        options_dict["table_name"] = options.table_name
        options_dict["delimiter"] = options.delimiter
        options_dict["no_headers"] = options.no_headers


    generate_schema_from_csv_file(options_dict["file_name"], options_dict["connection_string"],
                                  options_dict["table_name"], str(options_dict["delimiter"]),
                                  no_header=options_dict["no_headers"])