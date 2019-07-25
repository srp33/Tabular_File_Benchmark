import mmap
import sys
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
memory_map = sys.argv[4] == "True"

col_index_range = getColIndicesToQuery(col_names_file_path, memory_map)

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        for line in iter(my_file.readline, b""):
            line_items = line.rstrip(b"\n").split(b"\t")
            out_file.write(b"\t".join([line_items[i] for i in col_index_range]) + b"\n")

    my_file.close()
