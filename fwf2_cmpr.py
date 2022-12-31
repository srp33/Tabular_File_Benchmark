import mmap
import re
import sys
from Helper import *
import fastnumbers

compression_method = sys.argv[1]
compression_level = sys.argv[2]
query_type = sys.argv[3]
in_file_path = sys.argv[4]
out_file_path = sys.argv[5]
discrete_query_col_name = sys.argv[6].encode()
numeric_query_col_name = sys.argv[7].encode()
col_names_to_keep = sys.argv[8]

def filter_discrete_simple(row_indices, query_col_coords):
    matches = (b"AM", b"NZ")

    for row_index in row_indices:
        value = next(parse_data_values(0, 0, query_col_coords, parse_compressed_row(row_coords[row_index])))

        if value in matches:
            yield row_index

def filter_discrete_startsendswith(row_indices, query_col_coords):
    for row_index in row_indices:
        value = next(parse_data_values(0, 0, query_col_coords, parse_compressed_row(row_coords[row_index])))

        if value.startswith(b"A") or value.endswith(b"Z"):
            yield row_index

def filter_numeric(row_indices, query_col_coords):
    for row_index in row_indices:
        value = next(parse_data_values(0, 0, query_col_coords, parse_compressed_row(row_coords[row_index])))

        if fastnumbers.float(value) >= 0.1:
            yield row_index

def parse_compressed_row(row_coord):
    return cmpr.decompress(file_handles["data"][row_coord[1]:row_coord[2]]).rstrip()

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

in_file_path = f"{os.path.dirname(in_file_path)}/compressed/{os.path.basename(in_file_path)}.{compression_method}"

if compression_level != "NA":
    in_file_path = f"{in_file_path}_{compression_level}"

file_handles = {
    "cc": openReadFile(in_file_path, ".cc"),
    "cn": openReadFile(in_file_path, ".cn"),
    "data": openReadFile(in_file_path, ""),
    "rowstart": openReadFile(in_file_path, ".rowstart")
}

line_length = readIntFromFile(in_file_path, ".ll")
max_column_name_length = readIntFromFile(in_file_path, ".mcnl")
max_column_coord_length = readIntFromFile(in_file_path, ".mccl")
max_row_start_length = readIntFromFile(in_file_path, ".mrsl")
num_rows = int(len(file_handles["rowstart"]) / (max_row_start_length + 1)) - 1
row_coords = list(parse_data_coords(range(num_rows), file_handles["rowstart"], max_row_start_length, len(file_handles["data"])))

with open(out_file_path, 'wb') as out_file:
    if col_names_to_keep == "all_columns":
        column_names = []

        for i in range(0, len(file_handles["cn"]), max_column_name_length + 1):
            column_name = file_handles["cn"][i:(i + max_column_name_length)].rstrip(b" ")
            column_names.append(column_name)

            if column_name == discrete_query_col_name:
                discrete_query_col_index = int(i / (max_column_name_length + 1))
            if column_name == numeric_query_col_name:
                numeric_query_col_index = int(i / (max_column_name_length + 1))

        num_cols = int(len(file_handles["cn"]) / (max_column_name_length + 1))
        out_col_coords = list(parse_data_coords(range(num_cols), file_handles["cc"], max_column_coord_length, line_length))

        out_file.write(b"\t".join(column_names) + b"\n")
    else:
        col_names_to_keep2 = [name.encode() for name in col_names_to_keep.split(",")]

        column_names_to_find = {discrete_query_col_name, numeric_query_col_name} | set(col_names_to_keep2)
        column_name_indices = {}
        for i in range(0, len(file_handles["cn"]), max_column_name_length + 1):
            column_name = file_handles["cn"][i:(i + max_column_name_length)].rstrip(b" ")

            if column_name in column_names_to_find:
                column_name_indices[column_name] = int(i / (max_column_name_length + 1))

        discrete_query_col_index = column_name_indices[discrete_query_col_name]
        numeric_query_col_index = column_name_indices[numeric_query_col_name]

        out_col_indices = [column_name_indices[name] for name in col_names_to_keep2]
        out_col_coords = list(parse_data_coords(out_col_indices, file_handles["cc"], max_column_coord_length, line_length))

        out_file.write(b"\t".join(col_names_to_keep2) + b"\n")

    discrete_col_coords = parse_data_coords([discrete_query_col_index], file_handles["cc"], max_column_coord_length, line_length)
    numeric_col_coords = parse_data_coords([numeric_query_col_index], file_handles["cc"], max_column_coord_length, line_length)

    if query_type == "simple":
        keep_row_indices = filter_discrete_simple(range(num_rows), [next(discrete_col_coords)])
    elif query_type == "startsendswith":
        keep_row_indices = filter_discrete_startsendswith(range(num_rows), [next(discrete_col_coords)])

    keep_row_indices = filter_numeric(keep_row_indices, [next(numeric_col_coords)])

    for row_index in keep_row_indices:
        out_line = b"\t".join(parse_data_values(0, 0, out_col_coords, parse_compressed_row(row_coords[row_index])))
        out_file.write(out_line + b"\n")

for handle in file_handles:
    file_handles[handle].close()
