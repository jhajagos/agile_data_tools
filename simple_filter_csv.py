__author__ = 'jhajagos'

import csv
import sys


def main(file_name, column_number, value_to_exclude, delimiter):

    with open(file_name, "rb") as f:
        csv_reader = csv.reader(f, delimiter=delimiter)
        with open(file_name + ".filtered.csv", "wb") as fw:
            csv_writer = csv.writer(fw)
            i = 0
            i_rows_excluded = 0
            for row in csv_reader:
                if row[column_number] != value_to_exclude:
                    csv_writer.writerow(row)
                else:
                    i_rows_excluded += 1

                i += 1

            print("%s rows read and %s row filtered" % (i, i_rows_excluded))

if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4])