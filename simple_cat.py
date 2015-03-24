__author__ = 'jhajagos'

import sys
import glob


def main(in_files, has_header=True, out_file="simple_cat_result.txt", add_file_name=1, delimiter=","):

    with open(out_file, "w") as fw:
        i = 0
        for in_file in in_files:
            with open(in_file, "r") as f:
                j = 0
                for line in f:
                    if has_header:
                        if i == 0 or (i > 0 and j > 0):

                            if i == 0 and j == 0:
                                if add_file_name:
                                    line = line.lstrip()[:-1] + delimiter + "file_name" + "\n"

                            else:
                                if add_file_name:
                                    line = line.lstrip()[:-1] + delimiter + in_file + "\n"
                            fw.write(line)
                            j += 1
                        else:

                            j += 1
                    else:
                        if add_file_name:
                            line = line.lstrip()[:-1] + delimiter + in_file + "\n"
                        fw.write(line)
                        j += 1
                print("Read %s lines in '%s'" % (j, in_file))
                i += 1

if __name__ == "__main__":
    if "*" in sys.argv[1]:
        if len(sys.argv) == 1:
            main(glob.glob(sys.argv[1]))
        else:
            main(glob.glob(sys.argv[1]), has_header=bool(int(sys.argv[2])), out_file=sys.argv[3],
                 add_file_name=bool(int(sys.argv[4])), delimiter=sys.argv[5])
    else:
        main(sys.argv[1:])