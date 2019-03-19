import mmap
import sys
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
num_rows = int(sys.argv[4])
memory_map = True
chunk_size = 1000

col_indices = [x for x in getColIndicesToQuery(col_names_file_path, memory_map)]

with open(file_path + ".ll", 'rb') as ll_file:
    line_length = int(ll_file.read().rstrip())

with open(file_path + ".mccl", 'rb') as mccl_file:
    max_column_coord_length = int(mccl_file.read().rstrip())

with open(file_path + ".cc", 'rb') as cc_file:
    cc_map_file = mmap.mmap(cc_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(file_path, 'rb') as data_file:
        data_map_file = mmap.mmap(data_file.fileno(), 0, prot=mmap.PROT_READ)

        with open(out_file_path, 'wb') as out_file:
            row_indices = range(num_rows+1)
            num_cols = int(len(cc_map_file) / (max_column_coord_length + 1))

            col_coords = list(parse_data_coords(col_indices, cc_map_file, max_column_coord_length, line_length))

            out_lines = []

            for row_index in row_indices:
                out_lines.append(b"\t".join(parse_data_values(row_index, line_length, col_coords, data_map_file)).rstrip())

                if len(out_lines) % chunk_size == 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")
                    out_lines = []

            if len(out_lines) > 0:
                out_file.write(b"\n".join(out_lines) + b"\n")

        data_map_file.close()
    cc_map_file.close()
