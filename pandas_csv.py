import numpy as np
import pandas as pd
import sys

settings = sys.argv[1].split(",")
engine = settings[0].replace("_engine", "")
memory_map = False
if settings[1] == "memory_map":
    memory_map = True

query_type = sys.argv[2]
in_file_path = sys.argv[3]
out_file_path = sys.argv[4]
discrete_query_col_name = sys.argv[5]
numeric_query_col_name = sys.argv[6]
col_names_to_keep = sys.argv[7]

if col_names_to_keep == "all_columns":
    with open(in_file_path) as in_file:
        all_col_names = in_file.readline().rstrip("\n").split("\t")
else:
    all_col_names = [discrete_query_col_name, numeric_query_col_name] + col_names_to_keep.split(",")

# NOTE: Specifying the dtype of each column makes it somewhat faster, especially for very wide files.
dtypes = {discrete_query_col_name: "str", numeric_query_col_name: np.float64}
for col_name in all_col_names:
    if col_name.startswith("Discrete"):
        dtypes[col_name] = "str"
    else:
        dtypes[col_name] = np.float64

# pyarrow does not support na_filter = False. We set it to False for other engines because it should increase speeds for large files.
df = pd.read_csv(in_file_path, sep="\t", header=0, usecols=all_col_names, dtype=dtypes, na_filter=engine=="pyarrow", memory_map=memory_map, engine=engine)

if query_type == "simple":
    df = df[(df[discrete_query_col_name].isin(['AM', 'NZ'])) & (df[numeric_query_col_name] >= 0.1)]
elif query_type == "startsendswith":
    df = df[((df[discrete_query_col_name].str.startswith("A")) | (df[discrete_query_col_name].str.endswith("Z"))) & (df[numeric_query_col_name] >= 0.1)]

if col_names_to_keep != "all_columns":
    df = df[col_names_to_keep.split(",")]

df.to_csv(out_file_path, sep="\t", header=True, index=False, float_format='%.8f')
