import mmap
import sys
import re
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
memory_map = sys.argv[4] == "True"

# These have to be in sorted order
col_indices = getColIndicesToQuery(col_names_file_path, memory_map)

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        header_line = my_file.readline()
        header_items = header_line.rstrip(b"\n").split(b"\t")

        out_file.write(b"\t".join([header_items[i] for i in col_indices]) + b"\n")

        #reg_ex = r"^(?:[^\t]+\t){" + str(col_indices[0]) + "}([^\t]+)"
        reg_ex = r"^(?:[^\t]*\t){" + str(col_indices[0]) + "}([^\t]*)"

        for i in range(1, len(col_indices)):
            prior_index = col_indices[i-1]
            index = col_indices[i]

            reg_ex += r"(?:[^\t]*\t){" + str(index - prior_index) + "}([^\t]*)"

        reg_ex_comp = re.compile(reg_ex.encode())

        for line in iter(my_file.readline, b""):
            out_file.write(b"\t".join(reg_ex_comp.match(line).groups()).rstrip(b"\n") + b"\n")

    my_file.close()
