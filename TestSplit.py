import mmap
import sys
import re

file_path = sys.argv[1]
out_file_path = sys.argv[2]
memory_map = sys.argv[3] == "True"

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        header_items = my_file.readline().rstrip(b"\n").split(b"\t")

        index_range = range(0, len(header_items), 100)
        out_file.write(b"\t".join([header_items[i] for i in index_range]) + b"\n")

        #for line in my_file:
        for line in iter(my_file.readline, b""):
            line_items = line.rstrip(b"\n").split(b"\t")
            out_file.write(b"\t".join([line_items[i] for i in index_range]) + b"\n")

    my_file.close()
