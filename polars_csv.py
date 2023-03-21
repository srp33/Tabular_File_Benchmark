import polars as pl
import sys

query_type = sys.argv[1]
in_file_path = sys.argv[2]
out_file_path = sys.argv[3]
discrete_query_col_name = sys.argv[4]
numeric_query_col_name = sys.argv[5]
col_names_to_keep = sys.argv[6]

if col_names_to_keep == "all_columns":
    if query_type == "simple":
        pl.scan_csv(in_file_path, separator="\t").filter(pl.col(discrete_query_col_name).is_in(["AM", "NZ"])).filter(pl.col(numeric_query_col_name) >= 0.1).collect(streaming=True).write_csv(out_file_path, separator="\t")
    else:
        pl.scan_csv(in_file_path, separator="\t").filter(pl.col(discrete_query_col_name).str.starts_with("A") | pl.col(discrete_query_col_name).str.ends_with("Z")).filter(pl.col(numeric_query_col_name) >= 0.1).collect(streaming=True).write_csv(out_file_path, separator="\t")
else:
    if query_type == "simple":
        pl.scan_csv(in_file_path, separator="\t").filter(pl.col(discrete_query_col_name).is_in(["AM", "NZ"])).filter(pl.col(numeric_query_col_name) >= 0.1).select(col_names_to_keep.split(",")).collect(streaming=True).write_csv(out_file_path, separator="\t")
    else:
        pl.scan_csv(in_file_path, separator="\t").filter(pl.col(discrete_query_col_name).str.starts_with("A") | pl.col(discrete_query_col_name).str.ends_with("Z")).filter(pl.col(numeric_query_col_name) >= 0.1).select(col_names_to_keep.split(",")).collect(streaming=True).write_csv(out_file_path, separator="\t")

#if query_type == "simple":
#    df = df[(df[discrete_query_col_name].isin(['AM', 'NZ'])) & (df[numeric_query_col_name] >= 0.1)]
#elif query_type == "startsendswith":
#    df = df[((df[discrete_query_col_name].str.startswith("A")) | (df[discrete_query_col_name].str.endswith("Z"))) & (df[numeric_query_col_name] >= 0.1)]
