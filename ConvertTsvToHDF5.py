import numpy as np
import pandas as pd
import sys
from Helper import *

in_file_path = sys.argv[1]
out_file_path = sys.argv[2]
#complevel = int(sys.argv[3])
complevel = 0

with open(in_file_path) as in_file:
    column_names = in_file.readline().rstrip("\n").split("\t")

column_type_dict = {}
for column_name in column_names:
    if column_name.startswith("Numeric"):
        column_type_dict[column_name] = np.float64
    else:
        column_type_dict[column_name] = "str"

print("Reading " + in_file_path, flush=True)
df = pd.read_csv(in_file_path, sep="\t", header=0, dtype=column_type_dict, na_filter=False)

## Tried using format="table" because it supports selecting specific columns,
## but it threw an error saying we exceeded the column number limit.
print("Writing " + out_file_path, flush=True)
df.to_hdf(out_file_path, sep="\t", key="df", mode="w", format="fixed", complevel=complevel, dropna=False)
