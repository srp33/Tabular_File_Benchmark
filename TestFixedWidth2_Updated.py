import mmap
import sys
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
num_rows = int(sys.argv[4])
m_map = sys.argv[5]
chunk_size = 1000

if m_map == "MMAP":
    memory_map = True
elif m_map == "NO_MMAP":
    memory_map = False
else:
    print("Invalid argument for m_map, expected 'MMAP' or 'NO_MMAP' got '" + m_map + "'")
    exit(1)

col_indices = [x for x in getColIndicesToQuery(col_names_file_path, memory_map)]


with open(file_path + ".ll", 'rb') as ll_file:
    line_length = int(ll_file.read().rstrip())

with open(file_path + ".mccl", 'rb') as mccl_file:
    max_column_coord_length = int(mccl_file.read().rstrip())

with open(file_path + ".cc", 'rb') as cc_file:
    if memory_map:
        cc_file = mmap.mmap(cc_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(file_path, 'rb') as data_file:
        if memory_map:
            data_file = mmap.mmap(data_file.fileno(), 0, prot=mmap.PROT_READ)

        with open(out_file_path, 'wb') as out_file:
            row_indices = range(num_rows + 1)
            if memory_map:
                col_coords = list(parse_data_coords(col_indices, cc_file, max_column_coord_length, line_length))
            else:
                col_coords = list(parse_data_coords_seek(col_indices, file_path + ".cc", max_column_coord_length, line_length))

            out_lines = []
            if memory_map:
                for row_index in row_indices:
                    out_lines.append(b"\t".join(parse_data_values(row_index, line_length, col_coords, data_file)).rstrip())

                    if len(out_lines) % chunk_size == 0:
                        out_file.write(b"\n".join(out_lines) + b"\n")
                        out_lines = []
            else:
                for row_index in row_indices:
                    out_lines.append(b"\t".join(parse_data_values_seek(row_index, line_length, col_coords, file_path)).rstrip())

                    if len(out_lines) % chunk_size == 0:
                        out_file.write(b"\n".join(out_lines) + b"\n")
                        out_lines = []

            if len(out_lines) > 0:
                out_file.write(b"\n".join(out_lines) + b"\n")

        data_file.close()
    cc_file.close()
