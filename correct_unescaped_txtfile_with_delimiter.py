__author__ = 'jhajagos'
import sys
import csv


def main(file_name, delimiter):

    with open(file_name, "r") as f:

        first_line = f.next()

        positions = []

        string_to_parse = first_line
        last_position = 0
        while delimiter in string_to_parse:
            match_position = string_to_parse.index(delimiter)
            positions += [(last_position, last_position + match_position - 1)]
            string_to_parse = first_line[last_position + match_position + 1:]
            last_position = last_position + match_position + 1

    with open(file_name, "r") as f:

        with open(file_name + ".cleaned.csv", "wb") as fw:
            csv_writer = csv.writer(fw)
            for line in f:
                cells_to_write = []

                for pos in positions:
                    cells_to_write += [line[pos[0]:pos[1]+1].rstrip()]
                    last_position = pos[1]

                cells_to_write += [line[last_position + 2:-1].rstrip()]
                csv_writer.writerow(cells_to_write)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])