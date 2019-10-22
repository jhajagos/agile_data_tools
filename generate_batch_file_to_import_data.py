import argparse
import glob
import os


def main(glob_file_pattern, out_file_name):

    if ".bat" in out_file_name:
        line_ending = "\r\n"
    else:
        line_ending = "\n"

    files_to_add = glob.glob(glob_file_pattern)
    batch_string = ""

    script_directory = os.path.split(os.path.abspath(__file__))[0]
    import_script_file_name = os.path.join(script_directory, "bulk_import_data_from_csv_to_db_py3.py")

    for file_name in files_to_add:
        batch_string += 'python3 "%s" -j "%s"%s' % (import_script_file_name, file_name, line_ending)

    with open(out_file_name, "w") as fw:
        fw.write(batch_string)


if __name__ == "__main__":

    arg_parser_obj = argparse.ArgumentParser()

    arg_parser_obj.add_argument("-g", "--glob-file-pattern", dest="glob_file_pattern")
    arg_parser_obj.add_argument("-o", "--out-file-name", dest="out_file_name")

    arg_obj = arg_parser_obj.parse_args()

    main(arg_obj.glob_file_pattern, arg_obj.out_file_name)
