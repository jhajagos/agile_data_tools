__author__ = 'jhajagos'



"""
This will quickly generate an import of tables based on a template and file name pattern.
It uses the first part of the filename as the table name
"""

import sys
import glob
import os
import json

def main(file_name_pattern, template_json_file):

    files_to_import = glob.glob(file_name_pattern)

    csv_files_to_import = []
    for file_name in files_to_import:
        full_file_name = os.path.abspath(file_name)

        directory, file_name = os.path.split(full_file_name)
        base_file_name, extension = os.path.splitext(file_name)

        extension = extension.lower()
        if extension == ".csv":
            csv_files_to_import += [(file_name, base_file_name, directory)]

    print(csv_files_to_import)
    for csv_file_to_import in csv_files_to_import:
        with open(template_json_file, "r") as f:
            template_obj = json.load(f)

            file_name, base_file_name, directory = csv_file_to_import
            template_obj["file_name"] = os.path.join(directory, file_name)
            template_obj["table_name"] = base_file_name

            with open(os.path.join(directory, file_name) + ".json", "w") as fw:
                json.dump(template_obj, fw, indent=4, separators=(',', ': '))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
