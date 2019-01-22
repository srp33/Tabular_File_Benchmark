import msgpack
import os
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]

column_size_dict = {}
column_coord_dict = {}

# Initialize a dictionary with the column index as key and width of the column as value
with open(file_path, 'rb') as my_file:
    header_items = my_file.readline().rstrip(b"\n").split(b"\t")

    for i in range(len(header_items)):
        column_size_dict[i] = 0

# Iterate through the lines to find the max width for each column
with open(file_path, 'rb') as my_file:
    for line in my_file:
        line_items = line.rstrip(b"\n").split(b"\t")

        for i in range(len(line_items)):
            column_size_dict[i] = max([column_size_dict[i], len(line_items[i])])

# Calculate the length of the first line (and thus all the other lines)
line_length = sum([column_size_dict[i] for i in range(len(header_items))])

# Save value that indicates line length
with open(out_file_path + ".ll", 'wb') as ll_file:
    ll_file.write(str(line_length + 1).encode())

# Calculate the positions where each column starts and its length
cumulative_position = 0
for i in range(len(header_items)):
    column_size = column_size_dict[i]
    column_coord_dict[i] = "{},{}".format(cumulative_position, column_size)
    cumulative_position += column_size

# Save value that indicates maximum length of column coords string
max_column_coord_length = max([len(x) for x in set(column_coord_dict.values())])
with open(out_file_path + ".mccl", 'wb') as mccl_file:
    # Add one to account for newline character
    mccl_file.write(str(max_column_coord_length + 1).encode())

# Save column coords
with open(out_file_path + ".cc", 'wb') as cc_file:
    formatter = "{:<" + str(max_column_coord_length) + "}\n"
    for i, length in sorted(column_coord_dict.items()):
        cc_file.write(formatter.format(length).encode())

# Save the data to output file
with open(file_path, 'rb') as my_file:
    with open(out_file_path, 'wb') as out_file:
        line_number = 0
        cumulative_position = 0

        for line in my_file:
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

            line_number += 1
            cumulative_position += len(line_out)
