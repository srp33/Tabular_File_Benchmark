import msgpack
import mmap
import re
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]
memory_map = sys.argv[3] == "True"

#with open(file_path + ".coldict", 'rb') as coldict_file:
#    column_size_dict = msgpack.unpackb(coldict_file.read(), raw=False)

with open(file_path, 'rb') as my_file:
    header_line = my_file.readline().rstrip(b"\n") + b" "

header_items = re.findall(b"([^\s]+\s+)", header_line)
index_range = range(0, len(header_items), 100)

index_dict = {}
for i in index_range:
    start_index = header_line.find(header_items[i])
    stop_index = start_index + len(header_items[i])
    index_dict[i] = (start_index, stop_index)

def parse_substring(line, col_index):
    return b"".join(line[index_dict[col_index][0]:index_dict[col_index][1]].split())

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
#        while True:
#            line = my_file.readline()
#
#            if line == b"":
#                break
#
#            out_file.write(b"\t".join(parse_substring(line, i) for i in index_range) + b"\n")

        for line in iter(my_file.readline, b""):
            out_file.write(b"\t".join(parse_substring(line, i) for i in index_range) + b"\n")

    my_file.close()
