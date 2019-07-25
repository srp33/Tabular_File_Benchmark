import mmap
import sys
from Helper import *
import fastnumbers

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
num_rows = int(sys.argv[4])
query_col_indices = [int(x) for x in sys.argv[5].split(",")]
memory_map = True

def filter_rows(row_indices, query_col_index, query_col_coords):
    col_type = next(parse_data_values(query_col_index, max_column_type_length, [(query_col_index, 0, 1)], file_handles["ct"]))

    if col_type == b"n":
        for row_index in row_indices:
            if fastnumbers.float(next(parse_data_values(row_index, line_length, query_col_coords, file_handles["data"]))) >= 0.1:
                yield row_index
    else:
        for row_index in row_indices:
            value = next(parse_data_values(row_index, line_length, query_col_coords, file_handles["data"]))

            if value.startswith(b"A") or value.endswith(b"Z"):
                yield row_index

file_handles = {
    "cc": openReadFile(file_path, ".cc"),
    "data": openReadFile(file_path, ""),
    "ct": openReadFile(file_path, ".ct"),
}

line_length = readIntFromFile(file_path, ".ll")
max_column_coord_length = readIntFromFile(file_path, ".mccl")
max_column_type_length = readIntFromFile(file_path, ".mctl")
out_col_indices = [x for x in getColIndicesToQuery(col_names_file_path, memory_map)]
out_col_coords = list(parse_data_coords(out_col_indices, file_handles["cc"], max_column_coord_length, line_length))

with open(out_file_path, 'wb') as out_file:
    #num_cols = int(len(file_handles["cc"]) / (max_column_coord_length + 1))

    all_query_col_coords = parse_data_coords(query_col_indices, file_handles["cc"], max_column_coord_length, line_length)
    keep_row_indices = range(1, num_rows)

    for query_col_index in query_col_indices:
        keep_row_indices = filter_rows(keep_row_indices, query_col_index, [next(all_query_col_coords)])

    chunk_size = 1000
    out_lines = []

    for row_index in [0] + list(keep_row_indices):
        out_lines.append(b"\t".join(parse_data_values(row_index, line_length, out_col_coords, file_handles["data"])).rstrip())

        if len(out_lines) % chunk_size == 0:
            out_file.write(b"\n".join(out_lines) + b"\n")
            out_lines = []

    if len(out_lines) > 0:
        out_file.write(b"\n".join(out_lines) + b"\n")

for handle in file_handles:
    file_handles[handle].close()
