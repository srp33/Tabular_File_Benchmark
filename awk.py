import os
import sys

awk_command = sys.argv[1]
query_type = sys.argv[2]
in_file_path = sys.argv[3]
out_file_path = sys.argv[4]
discrete_query_col_name = sys.argv[5].encode()
numeric_query_col_name = sys.argv[6].encode()
col_names_to_keep = sys.argv[7]

with open(in_file_path, "rb") as in_file:
    header_line = in_file.readline()
    header_items = header_line.rstrip(b"\n").split(b"\t")
    discrete_query_col_index = header_items.index(discrete_query_col_name) + 1
    numeric_query_col_index = header_items.index(numeric_query_col_name) + 1

    with open(out_file_path, "wb") as out_file:
        if col_names_to_keep == "all_columns":
            col_indices_to_keep = list(range(len(header_items)))
            out_file.write(header_line)
        else:
            col_names_to_keep = [name.encode() for name in col_names_to_keep.split(",")]
            col_indices_to_keep = [header_items.index(name) for name in col_names_to_keep]
            out_file.write(b"\t".join(col_names_to_keep) + b"\n")

if query_type == "simple":
    command = awk_command + " -v OFS='\\t' '(($" + str(discrete_query_col_index) + "==\"AM\" || $" + str(discrete_query_col_index) + "==\"NZ\") && $" + str(numeric_query_col_index) + ">= 0.1) {print " + "$" + ",$".join([str(i + 1) for i in col_indices_to_keep]) + ";}' " + in_file_path + " >> " + out_file_path
elif query_type == "startsendswith":
    command = awk_command + " -v OFS='\\t' '(($" + str(discrete_query_col_index) + " ~ /^A/ || $" + str(discrete_query_col_index) + " ~ /Z$/) && $" + str(numeric_query_col_index) + ">= 0.1) {print " + "$" + ",$".join([str(i + 1) for i in col_indices_to_keep]) + ";}' " + in_file_path + " >> " + out_file_path

os.system(command)
