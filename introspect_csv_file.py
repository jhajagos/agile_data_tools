__author__ = 'jhajagos'

"""
For troubleshooting a CSV file
"""

import csv
import json
import sys


def main(file_name, delimiter=","):

    with open(file_name, "rb") as f:
        csv_reader = csv.reader(f, delimiter=delimiter)

        introspect_dict = {}
        i = 0
        for row in csv_reader:
            j = 0
            for cell in row:
                if j in introspect_dict:
                    pass
                else:
                    introspect_dict[j] = {}

                if cell in introspect_dict[j]:
                    introspect_dict[j][cell] += 1
                else:
                    introspect_dict[j][cell] = 1

                j += 1
            i += 1

    with open(file_name + ".introspect.json", "w") as fw:
        json.dump(introspect_dict, fw, indent=4, separators=(',', ': '))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])