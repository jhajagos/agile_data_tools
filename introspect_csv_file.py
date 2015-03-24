__author__ = 'jhajagos'

"""
For troubleshooting a CSV file
"""

import csv
import json
import sys


def main(file_name, delimiter=",", has_header=True, columns=None):

    with open(file_name, "rb") as f:
        csv_reader = csv.reader(f, delimiter=delimiter)

        if has_header:
            header = csv.reader.next()
        else:
            with open(file_name, "rb") as fs:
                csv_reader1 = csv.reader(f, delimiter=delimiter)
                header = range(len(csv_reader1.next()))


        if columns is None:
            columns = header

        introspect_dict = {}
        i = 0
        for row in csv_reader:
            for col in columns:
                col_pos = header.index(col)

                if col in introspect_dict:
                    pass
                else:
                    introspect_dict[col] = {}

                cell = row[col_pos]
                if cell in introspect_dict[col]:
                    introspect_dict[col][cell] += 1
                else:
                    introspect_dict[col][cell] = 1

            i += 1

    introspect_dict["_number_of_rows_"] = i
    with open(file_name + ".introspect.json", "w") as fw:
        json.dump(introspect_dict, fw, indent=4, separators=(',', ': '))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3:])