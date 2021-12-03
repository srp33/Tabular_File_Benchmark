import mmap
import sys
from Helper import *
import fastnumbers

file_path = sys.argv[1]
transposed_file_path = sys.argv[2]
col_names_file_path = sys.argv[3]
out_file_path = sys.argv[4]
query_col_indices = [int(x) for x in sys.argv[5].split(",")]
compression_method = sys.argv[6]
compression_level = sys.argv[7]
memory_map = True

if compression_method == "zstd":
    import zstandard
    print(dir(zstandard))
    cmpr = zstandard.ZstdDecompressor()
else:
    print("No matching compression method")
    sys.exit(1)

def filter_row(line, col_coords, query_col_index):
    col_type = next(parse_data_values(query_col_index, max_column_type_length, [(query_col_index, 0, 1)], file_handles["ct"]))

    if col_type == b"n":
        for coords in col_coords:
            if fastnumbers.float(line[coords[1]:coords[2]].rstrip()) >= 0.1:
                yield coords
    else:
        for coords in col_coords:
            value = line[coords[1]:coords[2]].rstrip()
            if value.startswith(b"A") or value.endswith(b"Z"):
                yield coords

line_length = readIntFromFile(file_path, ".ll")
t_line_length = readIntFromFile(transposed_file_path, ".ll")
max_column_coord_length = readIntFromFile(file_path, ".mccl")
t_max_column_coord_length = readIntFromFile(transposed_file_path, ".mccl")
max_row_start_length = readIntFromFile(file_path, ".mrsl")
#print(transposed_file_path + ".mrsl")
t_max_row_start_length = readIntFromFile(transposed_file_path, ".mrsl")
max_column_type_length = readIntFromFile(file_path, ".mctl")

file_handles = {
    "cc": openReadFile(file_path, ".cc"),
    "t_cc": openReadFile(transposed_file_path, ".cc"),
    "data": openReadFile(file_path, ""),
    "t_data": openReadFile(transposed_file_path, ""),
    "rowstart": openReadFile(file_path, ".rowstart"),
    "t_rowstart": openReadFile(transposed_file_path, ".rowstart"),
    "ct": openReadFile(file_path, ".ct"),
}

#num_variables = int(len(file_handles["cc"]) / (max_column_coord_length + 1))
num_samples = int(len(file_handles["t_cc"]) / (t_max_column_coord_length + 1))

t_filter_variable_coords = parse_data_coords(query_col_indices, file_handles["t_rowstart"], t_max_row_start_length, len(file_handles["t_data"]))

# I'm not sure yet why I need to specify the end_offset, but it works...
filter_lines = parse_data_values(0, t_line_length, t_filter_variable_coords, file_handles["t_data"], end_offset=1)

# Get the coordinates of all samples (before filtering)
t_sample_coords = parse_data_coords(range(num_samples), file_handles["t_cc"], t_max_column_coord_length, t_line_length)

# Iterate through each column that we want to use for filtering and filter the data
for query_col_index in query_col_indices:
    t_sample_coords = filter_row(cmpr.decompress(next(filter_lines)), t_sample_coords, query_col_index)

variable_indices = [x for x in getColIndicesToQuery(col_names_file_path, memory_map)]
variable_coords = list(parse_data_coords(variable_indices, file_handles["cc"], max_column_coord_length, line_length))

sample_coords = parse_data_coords([0] + [i[0] + 1 for i in t_sample_coords], file_handles["rowstart"], max_row_start_length, len(file_handles["data"]))

with open(out_file_path, 'wb') as out_file:
    chunk_size = 1000
    out_lines = []

    for sample_coord in sample_coords:
        # I'm not sure yet why I need to specify the end_offset, but it works...
        out_line = cmpr.decompress(next(parse_data_values(0, line_length, [sample_coord], file_handles["data"], end_offset=1)))
        out_items = parse_data_values(0, max_column_coord_length, variable_coords, out_line)
        out_lines.append(b"\t".join(out_items))

        if len(out_lines) % chunk_size == 0:
            out_file.write(b"\n".join(out_lines) + b"\n")
            out_lines = []

    if len(out_lines) > 0:
        out_file.write(b"\n".join(out_lines) + b"\n")

for handle in file_handles:
    file_handles[handle].close()
