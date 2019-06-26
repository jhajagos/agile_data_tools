from bulk_export_from_table import bulk_export_from_table
import json
import argparse

if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser(description="Bulk export table into CSV")
    arg_parse_obj.add_argument("-j", "--json-file-name", dest="json_file_name", required=True)

    arg_obj = arg_parse_obj.parse_args()

    json_file = arg_obj.json_file_name

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

        if "order_by" in table_config:
            order_by = table_config["order_by"]
        else:
            order_by = None

        bulk_export_from_table(connection_uri, file_name, table_name, restrictions=restrictions, schema=schema,
                               order_by=order_by)



