import mmap
import numpy as np
import pandas as pd
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]
memory_map = sys.argv[3] == "True"

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        header_items = my_file.readline().rstrip(b"\n").split(b"\t")
        column_indices = range(0, len(header_items), 100)
        column_names = [header_items[i].decode() for i in column_indices]

# NOTE: Specifying the dtype of each column makes it somewhat faster, especially for very wide files.
column_type_dict = {}
for column_name in column_names:
    if column_name.startswith("Numeric"):
        column_type_dict[column_name] = np.float64
    else:
        column_type_dict[column_name] = "str"

df = pd.read_csv(file_path, sep="\t", header=0, usecols=column_names, dtype=column_type_dict, memory_map=memory_map, na_filter=False)

df.to_csv(out_file_path, sep="\t", header=True, index=False, float_format='%.8f')
