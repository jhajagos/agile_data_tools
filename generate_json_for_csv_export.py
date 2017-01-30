import json
import sys
import time
import os

if __name__ == "__main__":

    if len(sys.argv) > 1:

        connection_uri = sys.argv[1]
        export_path = sys.argv[2]
        schema = sys.argv[3]
        tables = sys.argv[4:]

        table_export_list = []

        if schema == "None":
            schema = None

        for table in tables:

            time_stamp = time.strftime("%Y%m%d_%H%M%S")

            if schema is not None:
                table_name = schema + "." + table
            else:
                table_name = table

            file_name = os.path.join(export_path, table_name + "_" + time_stamp + ".csv")

            table_export_list += [{"connection_uri": connection_uri, "table_name": table_name, "schema": schema, "file_name": file_name}]

        print(json.dumps(table_export_list))

    else:

        print('Usage: python generate_json_for_csv_export.py "sqlite:///file.db3" "E:/data/export/" healthfacts table1 table2 table3 > table_export.json')







