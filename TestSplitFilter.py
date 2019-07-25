import mmap
import sys
from Helper import *
import fastnumbers

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
discrete_query_col_index = int(sys.argv[4])
num_query_col_index = int(sys.argv[5])
memory_map = True

col_index_range = getColIndicesToQuery(col_names_file_path, memory_map)

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        header_items = my_file.readline().rstrip(b"\n").split(b"\t")
        out_file.write(b"\t".join([header_items[i] for i in col_index_range]) + b"\n")

        match_count = 0

        for line in iter(my_file.readline, b""):
            line_items = line.rstrip(b"\n").split(b"\t")

            discrete_value = line_items[discrete_query_col_index]
            num_value = fastnumbers.float(line_items[num_query_col_index])

            if (discrete_value.startswith(b"A") or discrete_value.endswith(b"Z")) and num_value >= 0.1:
                out_file.write(b"\t".join([line_items[i] for i in col_index_range]) + b"\n")
                match_count += 1

    my_file.close()
