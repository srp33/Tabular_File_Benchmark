import msgpack
import mmap
import random
import re
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]

with open(file_path + ".rowdict", 'rb') as rowdict_file:
    row_start_dict = msgpack.unpackb(rowdict_file.read(), raw=False)

with open(file_path + ".coldict", 'rb') as coldict_file:
    col_coord_dict = msgpack.unpackb(coldict_file.read(), raw=False)

def parse_row_values(row_start):
    for col_index in [0] + select_col_indices:
        coords = col_coord_dict[col_index]
        yield mmap_file[(row_start + coords[0]):(row_start + coords[1])].rstrip()

random.seed(0)

select_row_indices = [i for i in row_start_dict.keys() if i != 0]
random.shuffle(select_row_indices)
select_col_indices = [i for i in col_coord_dict.keys() if i != 0]
random.shuffle(select_col_indices)
select_row_indices = sorted(select_row_indices[:10])
select_col_indices = sorted(select_col_indices[:10])

#NOTE: On the 500000x500000 file, 98.5% of the execution time occurred before this point.

with open(file_path, 'rb') as my_file:
    mmap_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        out_lines = []
        chunk_size = 100

        out_lines.append(b"\t".join(parse_row_values(row_start_dict[0])).rstrip())

        for row_index in select_row_indices:
            row_start = row_start_dict[row_index]
            out_lines.append(b"\t".join(parse_row_values(row_start)).rstrip())

            if len(out_lines) % chunk_size == 0:
                out_file.write(b"\n".join(out_lines) + b"\n")
                out_lines = []

        if len(out_lines) > 0:
            out_file.write(b"\n".join(out_lines) + b"\n")

    mmap_file.close()
