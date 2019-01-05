import csv
import io
import mmap
import pandas
import sys
import re

file_path = sys.argv[1]
out_file_path = sys.argv[2]
memory_map = sys.argv[3] == "True"

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        header_line = my_file.readline()
        header_items = header_line.rstrip(b"\n").split(b"\t")

        # These have to be in sorted order
        index_range = range(0, len(header_items), 100)
        indices = list(index_range)

        out_file.write(b"\t".join([header_items[i] for i in index_range]) + b"\n")

        #reg_ex = r"^(?:[^\t]+\t){" + str(indices[0]) + "}([^\t]+)"
        reg_ex = r"^(?:[^\t]*\t){" + str(indices[0]) + "}([^\t]*)"

        for i in range(1, len(indices)):
            prior_index = indices[i-1]
            index = indices[i]

            #reg_ex += r"(?:[^\t]+\t){" + str(index - prior_index) + "}([^\t]+)"
            reg_ex += r"(?:[^\t]*\t){" + str(index - prior_index) + "}([^\t]*)"

        reg_ex_comp = re.compile(reg_ex.encode())

        for line in iter(my_file.readline, b""):
            out_file.write(b"\t".join(reg_ex_comp.match(line).groups()).rstrip(b"\n") + b"\n")

    my_file.close()
