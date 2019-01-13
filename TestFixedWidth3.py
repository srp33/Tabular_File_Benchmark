import msgpack
import mmap
import re
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]
discrete_query_col_index = int(sys.argv[3])
num_query_col_index = int(sys.argv[4])

discrete_filter_criterion = b"MS"
num_filter_criterion = 0.9

# Loading these msgpack files takes 0.02-0.05 seconds for the medium-sized test files
with open(file_path + ".rowdict", 'rb') as rowdict_file:
    row_start_dict = msgpack.unpackb(rowdict_file.read(), raw=False)

with open(file_path + ".coldict", 'rb') as coldict_file:
    col_coord_dict = msgpack.unpackb(coldict_file.read(), raw=False)

def parse_row_value(col_index):
    coords = col_coord_dict[col_index]

    return mmap_file[row_start + coords[0]:row_start + coords[1]].rstrip()

def parse_row_values(row_start):
    for col_index in select_col_indices:
        coords = col_coord_dict[col_index]
        yield mmap_file[(row_start + coords[0]):(row_start + coords[1])].rstrip()

with open(file_path, 'rb') as my_file:
    mmap_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    matching_row_indices = set()

    for row_index in sorted(row_start_dict.keys())[1:]:
        row_start = row_start_dict[row_index]

        discrete_value = parse_row_value(discrete_query_col_index)
        num_value = float(parse_row_value(num_query_col_index))

        if discrete_value == discrete_filter_criterion:
            matching_row_indices.add(row_index)
        elif num_value > num_filter_criterion:
            matching_row_indices.add(row_index)

    select_col_indices = range(0, len(col_coord_dict), 100)

    with open(out_file_path, 'wb') as out_file:
        out_lines = []
        chunk_size = 100

        out_lines.append(b"\t".join(parse_row_values(row_start_dict[0])).rstrip())

        for row_index in sorted(list(matching_row_indices)):
            row_start = row_start_dict[row_index]
            out_lines.append(b"\t".join(parse_row_values(row_start)).rstrip())

            if len(out_lines) % chunk_size == 0:
                out_file.write(b"\n".join(out_lines) + b"\n")
                out_lines = []

        if len(out_lines) > 0:
            out_file.write(b"\n".join(out_lines) + b"\n")

    mmap_file.close()
