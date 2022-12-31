import mmap
import re
import sys
from Helper import *
import itertools
import fastnumbers
import zstandard

compression_method = sys.argv[1]
compression_level = sys.argv[2]
query_type = sys.argv[3]
in_file_path = sys.argv[4]
out_file_path = sys.argv[5]
discrete_query_col_name = sys.argv[6].encode()
numeric_query_col_name = sys.argv[7].encode()
col_names_to_keep = sys.argv[8]

def filter_discrete_simple(row_indices, query_values):
    matches = (b"AM", b"NZ")

    for row_index in row_indices:
        if query_values[row_index] in matches:
            yield row_index

def filter_discrete_startsendswith(row_indices, query_values):
    for row_index in row_indices:
        value = query_values[row_index]

        if value.startswith(b"A") or value.endswith(b"Z"):
            yield row_index

def filter_numeric(row_indices, query_values):
    for i, row_index in enumerate(row_indices):
        if fastnumbers.float(query_values[i]) >= 0.1:
            yield row_index

cmpr = zstandard.ZstdDecompressor()

portrait_in_file_path = f"{os.path.dirname(in_file_path)}/compressed/{os.path.basename(in_file_path)}.{compression_method}_{compression_level}"
landscape_in_file_path = f"{os.path.dirname(in_file_path)}/transposed_and_compressed/{os.path.basename(in_file_path)}.{compression_method}_{compression_level}"

portrait_file_handles = {
    "cc": openReadFile(portrait_in_file_path, ".cc"),
    "cn": openReadFile(portrait_in_file_path, ".cn"),
    "data": openReadFile(portrait_in_file_path, ""),
    "rowstart": openReadFile(portrait_in_file_path, ".rowstart")
}

landscape_file_handles = {
    "cc": openReadFile(landscape_in_file_path, ".cc"),
    "data": openReadFile(landscape_in_file_path, ""),
    "rowstart": openReadFile(landscape_in_file_path, ".rowstart")
}

portrait_line_length = readIntFromFile(portrait_in_file_path, ".ll")
portrait_max_column_name_length = readIntFromFile(portrait_in_file_path, ".mcnl")
portrait_max_column_coord_length = readIntFromFile(portrait_in_file_path, ".mccl")
portrait_max_row_start_length = readIntFromFile(portrait_in_file_path, ".mrsl")
portrait_num_rows = int(len(portrait_file_handles["rowstart"]) / (portrait_max_row_start_length + 1)) - 1

landscape_line_length = readIntFromFile(landscape_in_file_path, ".ll")
landscape_max_column_coord_length = readIntFromFile(landscape_in_file_path, ".mccl")
landscape_max_row_start_length = readIntFromFile(landscape_in_file_path, ".mrsl")

with open(out_file_path, 'wb') as out_file:
    if col_names_to_keep == "all_columns":
        column_names = []

        for i in range(0, len(portrait_file_handles["cn"]), portrait_max_column_name_length + 1):
            column_name = portrait_file_handles["cn"][i:(i + portrait_max_column_name_length)].rstrip(b" ")
            column_names.append(column_name)

            if column_name == discrete_query_col_name:
                discrete_query_col_index = int(i / (portrait_max_column_name_length + 1))
            if column_name == numeric_query_col_name:
                numeric_query_col_index = int(i / (portrait_max_column_name_length + 1))

        num_cols = int(len(portrait_file_handles["cn"]) / (portrait_max_column_name_length + 1))
        out_col_coords = list(parse_data_coords(range(num_cols), portrait_file_handles["cc"], portrait_max_column_coord_length, portrait_line_length))

        out_file.write(b"\t".join(column_names) + b"\n")
    else:
        col_names_to_keep2 = [name.encode() for name in col_names_to_keep.split(",")]

        column_names_to_find = {discrete_query_col_name, numeric_query_col_name} | set(col_names_to_keep2)
        column_name_indices = {}
        for i in range(0, len(portrait_file_handles["cn"]), portrait_max_column_name_length + 1):
            column_name = portrait_file_handles["cn"][i:(i + portrait_max_column_name_length)].rstrip(b" ")

            if column_name in column_names_to_find:
                column_name_indices[column_name] = int(i / (portrait_max_column_name_length + 1))

        discrete_query_col_index = column_name_indices[discrete_query_col_name]
        numeric_query_col_index = column_name_indices[numeric_query_col_name]

        out_col_indices = [column_name_indices[name] for name in col_names_to_keep2]
        out_col_coords = list(parse_data_coords(out_col_indices, portrait_file_handles["cc"], portrait_max_column_coord_length, portrait_line_length))

        out_file.write(b"\t".join(col_names_to_keep2) + b"\n")

    landscape_num_cols = int(len(landscape_file_handles["cc"]) / (landscape_max_column_coord_length + 1))
    landscape_col_coords = list(parse_data_coords(range(landscape_num_cols), landscape_file_handles["cc"], landscape_max_column_coord_length, landscape_line_length))

    discrete_col_coords = parse_data_coords([discrete_query_col_index], landscape_file_handles["rowstart"], landscape_max_row_start_length, len(landscape_file_handles["data"]))
    discrete_col_line = parse_data_values(0, landscape_line_length, discrete_col_coords, landscape_file_handles["data"], end_offset=1)
    discrete_col_line = cmpr.decompress(next(discrete_col_line))
    discrete_col_values = list(parse_data_values(0, 0, landscape_col_coords, discrete_col_line))

    if query_type == "simple":
        keep_row_indices = list(filter_discrete_simple(range(portrait_num_rows), discrete_col_values))
    elif query_type == "startsendswith":
        keep_row_indices = list(filter_discrete_startsendswith(range(portrait_num_rows), discrete_col_values))

    numeric_col_coords = parse_data_coords([numeric_query_col_index], landscape_file_handles["rowstart"], landscape_max_row_start_length, len(landscape_file_handles["data"]))
    numeric_col_line = parse_data_values(0, landscape_line_length, numeric_col_coords, landscape_file_handles["data"], end_offset=1)
    numeric_col_line = cmpr.decompress(next(numeric_col_line))

    landscape_col_coords2 = []
    for row_index in keep_row_indices:
        landscape_col_coords2.append(landscape_col_coords[row_index])

    numeric_col_values = list(parse_data_values(0, 0, landscape_col_coords2, numeric_col_line))

    keep_row_indices = filter_numeric(keep_row_indices, numeric_col_values)

    portrait_row_coords = parse_data_coords(keep_row_indices, portrait_file_handles["rowstart"], portrait_max_row_start_length, len(portrait_file_handles["data"]))

    for row_coord in portrait_row_coords:
        line = cmpr.decompress(portrait_file_handles["data"][row_coord[1]:row_coord[2]]).rstrip()

        out_line = b"\t".join(parse_data_values(0, 0, out_col_coords, line))
        out_file.write(out_line + b"\n")

for handle in portrait_file_handles:
    portrait_file_handles[handle].close()
for handle in landscape_file_handles:
    landscape_file_handles[handle].close()
