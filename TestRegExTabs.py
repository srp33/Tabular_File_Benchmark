import itertools
import mmap
import sys
import re

file_path = sys.argv[1]
out_file_path = sys.argv[2]
memory_map = sys.argv[3] == "True"

def add_column(i, end_character):
    if i in index_range:
        return r"([^\t]+" + end_character + ")"
    else:
        return r"(?:[^\t]+" + end_character + ")"

with open(file_path, 'r') as my_file:
    header_items = next(my_file).rstrip("\n").split("\t")
    index_range = range(0, len(header_items), 100)

    reg_ex = r"^"

    for i in range(len(header_items)-1):
        reg_ex += add_column(i, r"\t")

        if i > max(index_range):
            break

    if max(index_range) == (len(header_items) - 1):
        reg_ex += add_column(len(header_items)-1, r"\n")

    reg_ex_comp = re.compile(reg_ex.encode(), re.MULTILINE | re.DOTALL)

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file_text = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)
    else:
        my_file_text = my_file.read()

    with open(out_file_path, 'wb') as out_file:
        for match in reg_ex_comp.finditer(my_file_text):
            out_file.write(b"".join(match.groups()).rstrip(b"\t").rstrip(b"\n") + b"\n")

    my_file.close()
