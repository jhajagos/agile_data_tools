__author__ = 'jhajagos'

import sys
import glob


def main(in_files, out_file="simple_cat_result.txt"):
    with open(out_file, "w") as fw:
        for in_file in in_files:
            with open(in_file, "r") as f:
                for line in f:
                    fw.write(line)

if __name__ == "__main__":
    if "*" in sys.argv[1]:
        main(glob.glob(sys.argv[1]))
    else:
        main(sys.argv[1:])