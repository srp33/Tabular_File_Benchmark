import msgpack
import mmap
import re
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]
memory_map = sys.argv[3] == "True"

with open(file_path + ".rowdict", 'rb') as rowdict_file:
    row_start_dict = msgpack.unpackb(rowdict_file.read(), raw=False)

with open(file_path + ".coldict", 'rb') as coldict_file:
    col_coord_dict = msgpack.unpackb(coldict_file.read(), raw=False)

row_indices = sorted(row_start_dict.keys())
col_indices = range(0, len(col_coord_dict), 100)

def parse_row_values(row_index):
    row_start = row_start_dict[row_index]

    col_values = []

    for col_index in col_indices:
        pos_start = row_start + col_coord_dict[col_index][0]
        pos_end = row_start + col_coord_dict[col_index][1]
        col_values.append(mmap_file[pos_start:pos_end].rstrip())
        ## Use yield here??

    return col_values

with open(file_path, 'rb') as my_file:
    mmap_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        for row_index in row_indices:
            row_values = parse_row_values(row_index)
#            out_file.write(b"\t".join(parse_row_values(row_index)).rstrip() + b"\n")

    mmap_file.close()
