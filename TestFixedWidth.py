import mmap
import re
import sys
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
memory_map = sys.argv[4] == "True"

col_names = [x.decode() for x in getColNamesToQuery(col_names_file_path, memory_map)]

with open(file_path, 'rb') as my_file:
    header_line = my_file.readline().rstrip(b"\n").decode() + " "

# This is not a long-term solution, but it'll work for these tests.
index_dict = {}
for col_name in col_names:
    index_dict[col_name] = re.search(col_name + r"\s+", header_line).span()

def parse_substring(line, col_name):
    return b"".join(line[index_dict[col_name][0]:index_dict[col_name][1]].split())

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        for line in iter(my_file.readline, b""):
            out_file.write(b"\t".join(parse_substring(line, col_name) for col_name in col_names) + b"\n")

    my_file.close()
