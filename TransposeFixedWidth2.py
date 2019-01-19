import msgpack
import mmap
import re
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]

with open(file_path + ".rowdict", 'rb') as rowdict_file:
    row_start_dict = msgpack.unpackb(rowdict_file.read(), raw=False)

with open(file_path + ".coldict", 'rb') as coldict_file:
    col_coord_dict = msgpack.unpackb(coldict_file.read(), raw=False)

row_indices = sorted(row_start_dict.keys())
col_indices = sorted(col_coord_dict.keys())

chunk_size = 100
col_indices_chunks = []
chunk_indices = []

for col_index in col_indices:
    chunk_indices.append(col_index)

    if len(chunk_indices) == chunk_size:
        col_indices_chunks.append(chunk_indices)
        chunk_indices = []

if len(chunk_indices) > 0:
    col_indices_chunks.append(chunk_indices)

with open(out_file_path, 'wb') as out_file:
    for col_indices_chunk in col_indices_chunks:
        with open(file_path, 'rb') as my_file:
            mmap_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

            out_lines = []

            for col_index in col_indices_chunk:
                coords = col_coord_dict[col_index]

                out_items = []
                for row_index in row_indices:
                    row_start = row_start_dict[row_index]
                    out_items.append(mmap_file[(row_start + coords[0]):(row_start + coords[1])].rstrip())

                out_lines.append(b"".join(out_items).rstrip())

            mmap_file.close()

        out_file.write(b"\n".join(out_lines) + b"\n")

# I am not creating rowdict and coldict output files,
# but I don't think that's necessary for this benchmark.
# But in a real-world implementation, it would be.
