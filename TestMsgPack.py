import itertools
import sys
import mmap
import msgpack

file_path = sys.argv[1]
out_file_path = sys.argv[2]
memory_map = sys.argv[3] == "True"

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        unpacker = msgpack.Unpacker(my_file, use_list=False)

        header_items = next(unpacker)
        index_range = range(0, len(header_items), 100)
        header_items = [header_items[i] for i in index_range]
        out_file.write(b"\t".join(header_items) + b"\n")

        for unpacked in unpacker:
            out_file.write(b"\t".join(unpacked[i] for i in index_range) + b"\n")

    my_file.close()
