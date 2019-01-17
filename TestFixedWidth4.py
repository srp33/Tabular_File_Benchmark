import msgpack
import mmap
import re
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]
discrete_query_col_index = int(sys.argv[3])
num_query_col_index = int(sys.argv[4])
compression_method = sys.argv[5]
compression_level = sys.argv[6]

if compression_method == "bz2":
    import bz2 as cmpr
elif compression_method == "gz":
    import gzip as cmpr
elif compression_method == "lzma":
    import lzma as cmpr
elif compression_method == "snappy":
    import snappy as cmpr
else:
    print("No matching compression method")
    sys.exit(1)

discrete_filter_criterion = b"MS"
num_filter_criterion = 0.9

with open(file_path + ".rowdict", 'rb') as rowdict_file:
    row_start_dict = msgpack.unpackb(rowdict_file.read(), raw=False)

with open(file_path + ".coldict", 'rb') as coldict_file:
    col_coord_dict = msgpack.unpackb(coldict_file.read(), raw=False)

def parse_row_values(line):
    for col_index in select_col_indices:
        coords = col_coord_dict[col_index]
        yield line[coords[0]:coords[1]].rstrip()

with open(file_path, 'rb') as my_file:
    mmap_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    in_row_indices = sorted(row_start_dict.keys())

    discrete_query_col_coords = col_coord_dict[discrete_query_col_index]
    num_query_col_coords = col_coord_dict[num_query_col_index]

    matching_row_indices = set()

    for row_index in in_row_indices[1:]:
        row_start = row_start_dict[row_index]

        if row_index == in_row_indices[-1]:
            compressed_line = mmap_file[row_start:len(mmap_file)]
        else:
            row_end = row_start_dict[row_index + 1]
            compressed_line = mmap_file[row_start:row_end]

        line = cmpr.decompress(compressed_line)

        discrete_value = line[discrete_query_col_coords[0]:discrete_query_col_coords[1]].rstrip()
        num_value = float(line[num_query_col_coords[0]:num_query_col_coords[1]].rstrip())

        if discrete_value == discrete_filter_criterion:
            matching_row_indices.add(row_index)
        elif num_value > num_filter_criterion:
            matching_row_indices.add(row_index)

    select_col_indices = range(0, len(col_coord_dict), 100)

    with open(out_file_path, 'wb') as out_file:
        out_lines = []
        chunk_size = 100

        header_line = cmpr.decompress(mmap_file[row_start_dict[0]:row_start_dict[1]])

        out_lines.append(b"\t".join(parse_row_values(header_line)).rstrip())

        for row_index in sorted(list(matching_row_indices)):
            row_start = row_start_dict[row_index]

            if row_index == in_row_indices[-1]:
                compressed_line = mmap_file[row_start:len(mmap_file)]
            else:
                row_end = row_start_dict[row_index + 1]
                compressed_line = mmap_file[row_start:row_end]

            line = cmpr.decompress(compressed_line)

            out_lines.append(b"\t".join(parse_row_values(line)).rstrip())

            if len(out_lines) % chunk_size == 0:
                out_file.write(b"\n".join(out_lines) + b"\n")
                out_lines = []

        if len(out_lines) > 0:
            out_file.write(b"\n".join(out_lines) + b"\n")

    mmap_file.close()
