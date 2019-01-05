import mmap
import re
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]
memory_map = sys.argv[3] == "True"

with open(file_path, 'rb') as my_file:
    header_line = next(my_file).rstrip(b"\n")

header_items = re.split(b"\s+", header_line)[:-1]
index_range = range(0, len(header_items), 100)

index_dict = {}
last_start_index = 0
for i in range(len(header_items)):
    index_dict[i] = (last_start_index, last_start_index+15)
    last_start_index += 15

def parse_substring(line, col_index):
    return b"".join(line[index_dict[col_index][0]:index_dict[col_index][1]].split())

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
##        while True:
##            line = my_file.readline()
##
##            if line == b"":
##                break
##
##            out_file.write(b"\t".join(parse_substring(line, i) for i in index_range) + b"\n")

        for line in iter(my_file.readline, b""):
            out_file.write(b"\t".join(parse_substring(line, i) for i in index_range) + b"\n")
