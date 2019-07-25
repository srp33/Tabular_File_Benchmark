import sys

file_path = sys.argv[1]
dimensions = sys.argv[2]

with open(file_path) as my_file:
    file_lines = [line.strip() for line in my_file][1:]

    data = []
    for line in file_lines:
        line_items = line.split(": ")

        if line_items[1] != "0":
            print("{}\t{}\t{}".format(line_items[0], dimensions, line_items[1]))
