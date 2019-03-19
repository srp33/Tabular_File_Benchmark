import mmap
import sys
from Helper import *
import fastnumbers

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
num_rows = int(sys.argv[4])
query_col_indices = [int(x) for x in sys.argv[5].split(",")]
compression_method = sys.argv[6]
compression_level = sys.argv[7]
memory_map = True

if compression_method == "bz2":
    import bz2 as cmpr
elif compression_method == "gz":
    import gzip as cmpr
elif compression_method == "lzma":
    import lzma as cmpr
elif compression_method == "snappy":
    import snappy as cmpr
elif compression_method == "zstd":
    import zstandard
    cmpr = zstandard.ZstdDecompressor()
elif compression_method == "lz4":
    import lz4.frame as cmpr
else:
    print("No matching compression method")
    sys.exit(1)

def get_column_type(query_col_index):
    return next(parse_data_values(query_col_index, max_column_type_length, [(query_col_index, 0, 1)], file_handles["ct"]))

def parse_row(row_coord):
    return cmpr.decompress(file_handles["data"][row_coord[1]:row_coord[2]]).rstrip()

def is_match(value, value_type):
    if value_type == b"n":
        return fastnumbers.float(value) >= 0.1
    else:
        return value.startswith(b"A") or value.endswith(b"Z")

file_handles = {
    "cc": openReadFile(file_path, ".cc"),
    "data": openReadFile(file_path, ""),
    "ct": openReadFile(file_path, ".ct"),
    "rowstart": openReadFile(file_path, ".rowstart")
}

line_length = readIntFromFile(file_path, ".ll")
max_column_coord_length = readIntFromFile(file_path, ".mccl")
max_column_type_length = readIntFromFile(file_path, ".mctl")
max_row_start_length = readIntFromFile(file_path, ".mrsl")
out_col_indices = [x for x in getColIndicesToQuery(col_names_file_path, memory_map)]
out_col_coords = list(parse_data_coords(out_col_indices, file_handles["cc"], max_column_coord_length, line_length))

with open(out_file_path, 'wb') as out_file:
    num_cols = int(len(file_handles["cc"]) / (max_column_coord_length + 1))
    out_col_coords = list(parse_data_coords(out_col_indices, file_handles["cc"], max_column_coord_length, line_length))

    query_col_types = [get_column_type(query_col_index) for query_col_index in query_col_indices]
    query_col_coords = list(parse_data_coords(query_col_indices, file_handles["cc"], max_column_coord_length, line_length))

    num_rows = int(len(file_handles["rowstart"]) / (max_row_start_length + 1))
    row_coords = list(parse_data_coords(range(num_rows), file_handles["rowstart"], max_row_start_length, len(file_handles["data"])))

    chunk_size = 1000
    out_lines = []

    # Header line
    out_lines.append(b"\t".join(parse_data_values(0, 0, out_col_coords, parse_row(row_coords[0]).rstrip())))

    for row_coord in row_coords[1:]:
        line = parse_row(row_coord)

        value_generator = parse_data_values(0, 0, query_col_coords, line)
        value1 = next(value_generator)
        value_type1 = query_col_types[0]
        value2 = next(value_generator)
        value_type2 = query_col_types[1]

        num_matches = int(is_match(value1, value_type1)) + int(is_match(value2, value_type2))

        if num_matches == len(query_col_indices):
            out_lines.append(b"\t".join(parse_data_values(0, 0, out_col_coords, line)).rstrip())

            if len(out_lines) % chunk_size == 0:
                out_file.write(b"\n".join(out_lines) + b"\n")
                out_lines = []

    if len(out_lines) > 0:
        out_file.write(b"\n".join(out_lines) + b"\n")

for handle in file_handles:
    file_handles[handle].close()
