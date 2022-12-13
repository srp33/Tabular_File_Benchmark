import pandas as pd
import sys

query_type = sys.argv[1]
in_file_path = sys.argv[2]
out_file_path = sys.argv[3]
discrete_query_col_name = sys.argv[4]
numeric_query_col_name = sys.argv[5]
col_names_to_keep = sys.argv[6].split(",")

all_col_names = [discrete_query_col_name, numeric_query_col_name] + col_names_to_keep

#INFO: We cannot use the where argument because we cannot define "data columns" because in
#  the real world, we would not know ahead of time which columns would need to be queried.
df = pd.read_hdf(in_file_path, key="df", mode="r", columns = all_col_names)

if query_type == "simple":
    df = df[(df[discrete_query_col_name].isin(['AM', 'NZ'])) & (df[numeric_query_col_name] >= 0.1)][col_names_to_keep]
elif query_type == "startsendswith":
    df = df[((df[discrete_query_col_name].str.startswith("A")) | (df[discrete_query_col_name].str.endswith("Z"))) & (df[numeric_query_col_name] >= 0.1)][col_names_to_keep]

df.to_csv(out_file_path, sep="\t", header=True, index=False, float_format='%.8f')

# INFO: I tried this alternative way, but it was slower.

#df = pd.read_hdf(in_file_path, key="df", mode="r", columns = [discrete_query_col_name, numeric_query_col_name])
#
#if query_type == "simple":
#    matching_indices = df[(df[discrete_query_col_name].isin(['AM', 'NZ'])) & (df[numeric_query_col_name] >= 0.1)].index.tolist()
#elif query_type == "startsendswith":
#    matching_indices = df[((df[discrete_query_col_name].str.startswith("A")) | (df[discrete_query_col_name].str.endswith("Z"))) & (df[numeric_query_col_name] >= 0.1)].index
#
### This could be optimized further by looking for consecutive row indices,
## but it would likely not overcome the performance problems.
#
#i = matching_indices[0]
#df = pd.read_hdf(in_file_path, key="df", mode="r", columns = col_names_to_keep, start = i, stop = (i + 1))
#df.to_csv(out_file_path, sep="\t", header=True, index=False, float_format='%.8f', compression=None)
#
#for i in matching_indices[1:]:
#    df = pd.read_hdf(in_file_path, key="df", mode="r", columns = col_names_to_keep, start = i, stop = (i + 1))
#    df.to_csv(out_file_path, sep="\t", header=False, index=False, float_format='%.8f', mode="a", compression=None)
