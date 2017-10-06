from bulk_export_from_table import bulk_export_from_table
import json
import sys

if __name__ == "__main__":
    json_file = sys.argv[1]

    with open(json_file, "r") as f:
        tables_to_export_list = json.load(f)


    for table_config in tables_to_export_list:

        connection_uri = table_config["connection_uri"]
        file_name = table_config["file_name"]
        table_name = table_config["table_name"]

        if "restrictions" in table_config:
            restrictions = table_config["restrictions"]
        else:
            restrictions = None

        if "schema" in table_config:
            schema = table_config["schema"]
        else:
            schema = None

        bulk_export_from_table(connection_uri, file_name, table_name, restrictions=restrictions, schema=schema)



