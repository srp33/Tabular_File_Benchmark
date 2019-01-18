import msgpack
import mmap
import re
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]
memory_map = sys.argv[3] == "True"

# Loading these msgpack files takes 0.02-0.05 seconds for the medium-sized test files
with open(file_path + ".rowdict", 'rb') as rowdict_file:
    row_start_dict = msgpack.unpackb(rowdict_file.read(), raw=False)

with open(file_path + ".coldict", 'rb') as coldict_file:
    col_coord_dict = msgpack.unpackb(coldict_file.read(), raw=False)

row_indices = sorted(row_start_dict.keys())
col_indices = range(0, len(col_coord_dict), 100)

def parse_row_values(row_start):
    for col_index in col_indices:
        coords = col_coord_dict[col_index]
        yield mmap_file[(row_start + coords[0]):(row_start + coords[1])].rstrip()

with open(file_path, 'rb') as my_file:
    mmap_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        out_lines = []
        chunk_size = 100

        for row_index in row_indices:
            row_start = row_start_dict[row_index]
            out_lines.append(b"\t".join(parse_row_values(row_start)).rstrip())

            if len(out_lines) % chunk_size == 0:
                out_file.write(b"\n".join(out_lines) + b"\n")
                out_lines = []

        if len(out_lines) > 0:
            out_file.write(b"\n".join(out_lines) + b"\n")

    mmap_file.close()
