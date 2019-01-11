import mmap
import sys
import re

file_path = sys.argv[1]
out_file_path = sys.argv[2]

def transpose(x):
    return list(map(list, zip(*x)))

with open(file_path, 'rb') as my_file:
    my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    header_items = my_file.readline().rstrip(b"\n").split(b"\t")

    index_range = range(0, len(header_items), 100)

    data = [[header_items[i] for i in index_range]]

    for line in iter(my_file.readline, b""):
        line_items = line.rstrip(b"\n").split(b"\t")
        data.append([line_items[i] for i in index_range])

    my_file.close()

    data = transpose(data)

    with open(out_file_path, 'wb') as out_file:
        for line_items in data:
            out_file.write(b"\t".join(line_items) + b"\n")
