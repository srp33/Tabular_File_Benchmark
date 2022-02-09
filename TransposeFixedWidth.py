import mmap
import sys
from Helper import *

file_path = sys.argv[1]
out_file_path = sys.argv[2]

out_column_size_dict = {}

with open(file_path, 'rb') as data_file:
    num_rows = 0
    for line in data_file:
        out_column_size_dict[num_rows] = 0
        num_rows += 1

with open(file_path + ".cc", 'rb') as data_file:
    num_cols = -1
    for line in data_file:
        num_cols += 1

row_indices = list(range(num_rows))
col_indices = list(range(num_cols))

with open(file_path + ".ll", 'rb') as ll_file:
    line_length = int(ll_file.read().rstrip())

with open(file_path + ".mccl", 'rb') as mccl_file:
    max_column_coord_length = int(mccl_file.read().rstrip())

with open(file_path + ".cc", 'rb') as cc_file:
    cc_map_file = mmap.mmap(cc_file.fileno(), 0, prot=mmap.PROT_READ)
    col_coords = list(parse_data_coords(col_indices, cc_map_file, max_column_coord_length, line_length))

    # Find the sizes of the output columns
    with open(file_path, 'rb') as data_file:
        data_map_file = mmap.mmap(data_file.fileno(), 0, prot=mmap.PROT_READ)

        for row_index in row_indices[1:]:
            if row_index > 0 and row_index % 1000 == 0:
                print("Finding the sizes of the output columns for row {}.".format(row_index), flush=True)

            for value in parse_data_values(row_index, line_length, col_coords, data_map_file):
                out_column_size_dict[row_index] = max([out_column_size_dict[row_index], len(value)])

        out_column_formatter_dict = {}
        for i, size in out_column_size_dict.items():
            out_column_formatter_dict[i] = "{:" + str(size) + "}"

        # Parse and save the transposed data to the output file
        with open(out_file_path, 'wb') as out_file:
            for col_index in col_indices:
                if col_index > 0 and col_index % 1000 == 0:
                    print("Parsing and saving the transposed data to the output file for column {}.".format(col_index), flush=True)

                out_items = []
                for row_index in row_indices[1:]:
                    value = parse_data_values(row_index, line_length, col_coords[col_index:(col_index+1)], data_map_file)
                    out_items.append(out_column_formatter_dict[row_index].format(next(value).decode()))

                out_file.write("".join(out_items).encode() + b"\n")

        data_map_file.close()
    cc_map_file.close()

# Calculate the length of the first line (and thus all the other lines)
out_line_length = sum([out_column_size_dict[i] for i in range(len(out_column_size_dict))])

# Save value that indicates line length
with open(out_file_path + ".ll", 'wb') as out_ll_file:
    out_ll_file.write(str(out_line_length + 1).encode())

# Calculate the positions where each column starts
out_column_start_coords = []
cumulative_position = 0
for row_index in row_indices[1:]:
    column_size = out_column_size_dict[row_index]
    out_column_start_coords.append(str(cumulative_position))
    cumulative_position += column_size

# Calculate the column coordinates and max length of these coordinates
out_column_coords_string, out_max_column_coord_length = buildStringMap(out_column_start_coords)

# Save column coordinates
with open(out_file_path + ".cc", 'wb') as out_cc_file:
    out_cc_file.write(out_column_coords_string)

# Save value that indicates maximum length of column coords string
with open(out_file_path + ".mccl", 'wb') as out_mccl_file:
    out_mccl_file.write(out_max_column_coord_length)
