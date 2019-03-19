import numpy as np
import pandas as pd
import sys
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]
memory_map = sys.argv[4] == "True"

column_names = [x.decode() for x in getColNamesToQuery(col_names_file_path, memory_map)]

df = pd.read_hdf(file_path, key="df", mode="r", na_filter=False)

df[column_names].to_csv(out_file_path, sep="\t", header=True, index=False, float_format='%.8f')
