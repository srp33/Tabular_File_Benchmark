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

df = pd.read_csv(in_file_path, sep="\t", header=0, dtype=column_type_dict, na_filter=False)

## format="table" is necessary to support projecting specific columns.
#df.to_hdf(out_file_path, key="df", mode="w", format="fixed", complevel=complevel, dropna=False)
#PerformanceWarning: table /df/table is exceeding the recommended maximum number of columns (512); be ready to see PyTables asking for *lots* of memory and possibly slow I/O
#df.to_hdf(out_file_path, key="df", mode="w", format="table", data_columns=True, complevel=complevel, dropna=False)
df.to_hdf(out_file_path, key="df", mode="w", format="table", complevel=complevel, dropna=False)
