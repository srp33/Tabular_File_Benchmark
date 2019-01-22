import msgpack
import mmap
import re
import sys

file_path = sys.argv[1]
dimensions = int(sys.argv[2])
out_file_path = sys.argv[3]

def find_col_coords(col_indices):
    for col_index in col_indices:
        start_pos = col_index * max_column_coord_length
        next_start_pos = start_pos + max_column_coord_length

        yield [int(x) for x in cc_map_file[start_pos:next_start_pos].rstrip().split(b",")]

with open(file_path + ".ll", 'rb') as ll_file:
    line_length = int(ll_file.read().rstrip())

with open(file_path + ".mccl", 'rb') as mccl_file:
    max_column_coord_length = int(mccl_file.read().rstrip())

with open(file_path + ".cc", 'rb') as cc_file:
    cc_map_file = mmap.mmap(cc_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        row_indices = list(range(dimensions))
        col_indices = list(range(dimensions))

        #chunk_size = 1000
        chunk_size = 5
        col_indices_chunks = []
        chunk_indices = []

        for col_index in col_indices:
            chunk_indices.append(col_index)

            if len(chunk_indices) == chunk_size:
                col_indices_chunks.append(chunk_indices)
                chunk_indices = []

        if len(chunk_indices) > 0:
            col_indices_chunks.append(chunk_indices)

        for col_indices_chunk in col_indices_chunks:
            with open(file_path, 'rb') as data_file:
                data_map_file = mmap.mmap(data_file.fileno(), 0, prot=mmap.PROT_READ)

                col_coords = list(find_col_coords(col_indices_chunk))
                out_lines = []

                for i in range(len(col_indices_chunk)):
                    col_index = col_indices_chunk[i]
                    coords = col_coords[i]

                    out_items = []
                    for row_index in row_indices:
                        row_start = row_index * line_length

                        out_items.append(data_map_file[(row_start + coords[0]):(row_start + coords[0] + coords[1])].rstrip())

                    out_lines.append(b"".join(out_items).rstrip())

                    if len(out_lines) % chunk_size == 0:
                        out_file.write(b"\n".join(out_lines) + b"\n")
                        out_lines = []

                if len(out_lines) > 0:
                    out_file.write(b"\n".join(out_lines) + b"\n")

                data_map_file.close()
    cc_map_file.close()

# I am not creating output index files,
# but I don't think that's necessary for this benchmark.
# But in a real-world implementation, it would be.
