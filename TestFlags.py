# https://stackoverflow.com/questions/1883980/find-the-nth-occurrence-of-substring-in-a-string
import mmap
import sys
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
memory_map = sys.argv[4] == "True"

col_indices = getColIndicesToQuery(col_names_file_path, memory_map)

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        for line in iter(my_file.readline, b""):
            out_items = []
            token_index = 0
            for i in col_indices:
                token = "@{}@".format(i).encode()
                token_index = line.find(token, token_index)
                data_index = token_index + len(token)
                token_index = line.find(b"\t", data_index)
                if token_index == -1:
                    out_items.append(line[data_index:-1])
                else:
                    out_items.append(line[data_index:token_index])

            out_file.write(b"\t".join(out_items) + b"\n")

    my_file.close()
