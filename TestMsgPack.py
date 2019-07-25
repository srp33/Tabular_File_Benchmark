import itertools
import sys
import mmap
import msgpack
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
memory_map = sys.argv[4] == "True"

col_indices = getColIndicesToQuery(col_names_file_path, memory_map)

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        unpacker = msgpack.Unpacker(my_file, use_list=False)

        header_items = next(unpacker)
        header_items = [header_items[i] for i in col_indices]
        out_file.write(b"\t".join(header_items) + b"\n")

        for unpacked in unpacker:
            out_file.write(b"\t".join(unpacked[i] for i in col_indices) + b"\n")

    my_file.close()
