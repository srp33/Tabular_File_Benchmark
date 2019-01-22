import mmap
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]
num_rows = int(sys.argv[3])
discrete_query_col_index = int(sys.argv[4])
num_query_col_index = int(sys.argv[5])

def find_col_coords(col_indices):
    for col_index in col_indices:
        start_pos = col_index * max_column_coord_length
        next_start_pos = start_pos + max_column_coord_length

        yield [int(x) for x in cc_map_file[start_pos:next_start_pos].rstrip().split(b",")]

def parse_row_values(row_index, col_coords):
    row_start = row_index * line_length

    for coords in col_coords:
        yield data_map_file[row_start + coords[0]:row_start + coords[0] + coords[1]].rstrip()

def query_discrete_col(row_indices):
    matching_row_indices = []

    coords = list(find_col_coords([discrete_query_col_index]))[0]

    for row_index in row_indices:
        row_start = row_index * line_length
        discrete_value = data_map_file[row_start + coords[0]:row_start + coords[0] + coords[1]].rstrip()

        if discrete_value.startswith(b"A") or discrete_value.endswith(b"Z"):
            matching_row_indices.append(row_index)

    return matching_row_indices

def query_numeric_col(row_indices):
    matching_row_indices = []

    coords = list(find_col_coords([num_query_col_index]))[0]

    for row_index in row_indices:
        row_start = row_index * line_length
        num_value = float(data_map_file[row_start + coords[0]:row_start + coords[0] + coords[1]].rstrip())

        if num_value >= 0.1:
            matching_row_indices.append(row_index)

    return matching_row_indices

with open(file_path + ".ll", 'rb') as ll_file:
    line_length = int(ll_file.read().rstrip())

with open(file_path + ".mccl", 'rb') as mccl_file:
    max_column_coord_length = int(mccl_file.read().rstrip())

with open(file_path + ".cc", 'rb') as cc_file:
    cc_map_file = mmap.mmap(cc_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(file_path, 'rb') as data_file:
        data_map_file = mmap.mmap(data_file.fileno(), 0, prot=mmap.PROT_READ)

        with open(out_file_path, 'wb') as out_file:
            num_cols = int(len(cc_map_file) / max_column_coord_length)
            out_col_indices = range(0, num_cols, 100)
            out_col_coords = list(find_col_coords(out_col_indices))

            # Header line
            out_file.write(b"\t".join(parse_row_values(0, out_col_coords)).rstrip() + b"\n")

            matching_row_indices = query_discrete_col(range(1, num_rows))
            matching_row_indices = query_numeric_col(matching_row_indices)

            chunk_size = 1000
            out_lines = []

            for row_index in matching_row_indices:
                out_lines.append(b"\t".join(parse_row_values(row_index, out_col_coords)).rstrip())

                if len(out_lines) % chunk_size == 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
                    out_lines = []

            if len(out_lines) > 0:
                out_file.write(b"\n".join(out_lines) + b"\n")

        data_map_file.close()
    cc_map_file.close()
