import gzip
import os
import sys
from Helper import *
from fastnumbers import *

tsv_file_path = sys.argv[1]
out_file_path = sys.argv[2]

column_size_dict = {}
column_start_coords = []

def get_tsv_file_handle():
    if tsv_file_path.endswith(".gz"):
        return gzip.open(tsv_file_path)
    else:
        return open(tsv_file_path, 'rb')

# Initialize a dictionary with the column index as key and width of the column as value
tsv_file = get_tsv_file_handle()

column_names = tsv_file.readline().rstrip(b"\n").split(b"\t")

column_names_string, max_column_name_length = buildStringMap([x.decode() for x in column_names])
with open(out_file_path + ".cn", 'wb') as col_names_file:
    col_names_file.write(column_names_string)
with open(out_file_path + ".mcnl", 'wb') as mcnl_file:
    mcnl_file.write(max_column_name_length)

# Iterate through the lines to find the max width of each column
for i in range(len(column_names)):
    column_size_dict[i] = 0

for line in tsv_file:
    line_items = line.rstrip(b"\n").split(b"\t")

    for i in range(len(line_items)):
        column_size_dict[i] = max([column_size_dict[i], len(line_items[i])])

tsv_file.close()

# Calculate the length of the first line (and thus all the other lines)
line_length = sum([column_size_dict[i] for i in range(len(column_names))])

# Save value that indicates line length
with open(out_file_path + ".ll", 'wb') as ll_file:
    ll_file.write(str(line_length + 1).encode())

# Calculate the positions where each column starts
cumulative_position = 0
for i in range(len(column_names)):
    column_size = column_size_dict[i]
    column_start_coords.append(str(cumulative_position))
    cumulative_position += column_size
column_start_coords.append(str(line_length + 1))

# Calculate the column coordinates and max length of these coordinates
column_coords_string, max_column_coord_length = buildStringMap(column_start_coords)

# Save column coordinates
with open(out_file_path + ".cc", 'wb') as cc_file:
    cc_file.write(column_coords_string)

# Save value that indicates maximum length of column coords string
with open(out_file_path + ".mccl", 'wb') as mccl_file:
    mccl_file.write(max_column_coord_length)

# Save the data to output file
tsv_file = get_tsv_file_handle()
tsv_file.readline()

with open(out_file_path, 'wb') as out_file:
    for line in tsv_file:
        line_items = line.rstrip(b"\n").split(b"\t")

        line_out = ""
        for i in sorted(column_size_dict.keys()):
            format_string = "{:<" + str(column_size_dict[i]) + "}"
            column_value = format_string.format(line_items[i].decode())
            line_out += column_value

        # This newline character is unnecessary, so it adds a bit of disk space.
        # However, it makes the files much more readable to humans.
        line_out = (line_out + "\n").encode()
        out_file.write(line_out)

tsv_file.close()
