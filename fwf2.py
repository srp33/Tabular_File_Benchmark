import mmap
import re
import sys
from Helper import *
import fastnumbers

query_type = sys.argv[1]
in_file_path = sys.argv[2]
out_file_path = sys.argv[3]
discrete_query_col_name = sys.argv[4].encode()
numeric_query_col_name = sys.argv[5].encode()
col_names_to_keep = [name.encode() for name in sys.argv[6].split(",")]

memory_map = True

def filter_discrete_simple(row_indices, query_col_coords):
    matches = (b"AM", b"NZ")

    for row_index in row_indices:
        value = next(parse_data_values(row_index, line_length, query_col_coords, file_handles["data"]))

        if value in matches:
            yield row_index

def filter_discrete_startsendswith(row_indices, query_col_coords):
    for row_index in row_indices:
        value = next(parse_data_values(row_index, line_length, query_col_coords, file_handles["data"]))

        if value.startswith(b"A") or value.endswith(b"Z"):
            yield row_index

def filter_numeric(row_indices, query_col_coords):
    for row_index in row_indices:
        value = next(parse_data_values(row_index, line_length, query_col_coords, file_handles["data"]))

        if fastnumbers.float(value) >= 0.1:
            yield row_index

file_handles = {
    "cc": openReadFile(in_file_path, ".cc"),
    "cn": openReadFile(in_file_path, ".cn"),
    "ct": openReadFile(in_file_path, ".ct"),
    "data": openReadFile(in_file_path, ""),
}

line_length = readIntFromFile(in_file_path, ".ll")
max_column_name_length = readIntFromFile(in_file_path, ".mcnl")
max_column_coord_length = readIntFromFile(in_file_path, ".mccl")
max_column_type_length = readIntFromFile(in_file_path, ".mctl")
num_rows = int(len(file_handles["data"]) / line_length)

column_name_indices = {}
for i in range(0, len(file_handles["cn"]), max_column_name_length + 1):
    column_name = file_handles["cn"][i:(i + max_column_name_length)].rstrip(b" ")
    column_name_indices[column_name] = int(i / (max_column_name_length + 1))

out_col_indices = [column_name_indices[name] for name in col_names_to_keep]
out_col_coords = list(parse_data_coords(out_col_indices, file_handles["cc"], max_column_coord_length, line_length))

discrete_query_col_index = column_name_indices[discrete_query_col_name]
discrete_col_coords = parse_data_coords([discrete_query_col_index], file_handles["cc"], max_column_coord_length, line_length)

numeric_query_col_index = column_name_indices[numeric_query_col_name]
numeric_col_coords = parse_data_coords([numeric_query_col_index], file_handles["cc"], max_column_coord_length, line_length)

with open(out_file_path, 'wb') as out_file:
    out_file.write(b"\t".join(col_names_to_keep) + b"\n")

    if query_type == "simple":
        keep_row_indices = filter_discrete_simple(range(num_rows), [next(discrete_col_coords)])
    elif query_type == "startsendswith":
        keep_row_indices = filter_discrete_startsendswith(range(num_rows), [next(discrete_col_coords)])

    keep_row_indices = filter_numeric(keep_row_indices, [next(numeric_col_coords)])

    #chunk_size = 1000
    out_lines = []

    for row_index in keep_row_indices:
        out_line = b"\t".join(parse_data_values(row_index, line_length, out_col_coords, file_handles["data"]))
        out_file.write(out_line + b"\n")

#        if len(out_lines) % chunk_size == 0:
#            out_file.write(b"\n".join(out_lines) + b"\n")
#            out_lines = []

#    if len(out_lines) > 0:
#        out_file.write(b"\n".join(out_lines) + b"\n")

for handle in file_handles:
    file_handles[handle].close()
