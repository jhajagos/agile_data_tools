import argparse
import csv


def main(in_csv_file_name, out_csv_file_name, field_for_key, delimiter=','):

    i = 0
    field_value_dict = {}

    with open(in_csv_file_name, "rb") as f:
        csv_reader = csv.reader(f)
        with open(out_csv_file_name, "wb") as fw:
            csv_writer = csv.writer(fw)
            for row in csv_reader:

                if i == 0:
                    header = row
                    field_index = header.index(field_for_key)
                    csv_writer.writerow(header)
                else:
                    field_value = row[field_index]

                    if field_value not in field_value_dict:
                        csv_writer.writerow(row)
                        field_value_dict[field_value] = 1
                i += 1


if __name__ == "__main__":

    arg_parse_obj = argparse.ArgumentParser()

    arg_parse_obj.add_argument("-f", dest="in_csv_file_name")
    arg_parse_obj.add_argument("-o", dest="out_csv_file_name")
    arg_parse_obj.add_argument("-k", dest="field_key")

    arg_obj = arg_parse_obj.parse_args()

    main(arg_obj.in_csv_file_name, arg_obj.out_csv_file_name, field_for_key=arg_obj.field_key)