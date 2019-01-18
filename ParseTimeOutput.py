import sys

file_path = sys.argv[1]
description = sys.argv[2]
dimensions = sys.argv[3]

with open(file_path) as my_file:
    file_lines = [line.strip() for line in my_file][1:]

    data = []
    for line in file_lines:
        line_items = line.split(": ")
        if line_items[1] != "0":
            print("{}\t{}\t".format(description, dimensions) + "\t".join(line_items))
