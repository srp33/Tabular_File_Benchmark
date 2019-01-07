import msgpack
import os
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]

column_size_dict = {}
column_coord_dict = {}

# Build a dictionary with the column index as key and width of the column as value
with open(file_path, 'rb') as my_file:
    header_items = my_file.readline().rstrip(b"\n").split(b"\t")

    for i in range(len(header_items)):
        column_size_dict[i] = 0

# Iterate through the lines to find the max width for each column
with open(file_path, 'rb') as my_file:
    for line in my_file:
        line_items = line.rstrip(b"\n").split(b"\t")

        for i in range(len(line_items)):
            column_size_dict[i] = max([column_size_dict[i], len(line_items[i]) + 1])

# Serialize and save dictionary that indicates where each column starts and ends
with open(out_file_path + ".coldict", 'wb') as coldict_file:
    for i in range(len(header_items)):
        value = header_items[i]
        #TODO: Finish this

    coldict_file.write(msgpack.packb(column_size_dict, use_bin_type=True))

# Save the data to an output file with the proper padding
with open(file_path, 'rb') as my_file:
    with open(out_file_path, 'wb') as out_file:
        for line in my_file:
            line_items = line.rstrip(b"\n").split(b"\t")

            line_out = ""
            for i in sorted(column_size_dict.keys()):
                format_string = "{:<" + str(column_size_dict[i]) + "}"
                column_value = format_string.format(line_items[i].decode())
                line_out += column_value

            out_file.write(line_out[:-1].encode() + b"\n")
