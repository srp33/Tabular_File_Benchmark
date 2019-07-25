import numpy as np
import pandas as pd
import sys
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
memory_map = sys.argv[4] == "True"

column_names = [x.decode() for x in getColNamesToQuery(col_names_file_path, memory_map)]

# NOTE: Specifying the dtype of each column makes it somewhat faster, especially for very wide files.
column_type_dict = {}
for column_name in column_names:
    if column_name.startswith("Numeric"):
        column_type_dict[column_name] = np.float64
    else:
        column_type_dict[column_name] = "str"

df = pd.read_csv(file_path, sep="\t", header=0, usecols=column_names, dtype=column_type_dict, memory_map=memory_map, na_filter=False)

df.to_csv(out_file_path, sep="\t", header=True, index=False, float_format='%.8f')
