import duckdb
import string
import sys

query_type = sys.argv[1]
in_file_path = sys.argv[2]
out_file_path = sys.argv[3]
discrete_query_col_name = sys.argv[4]
numeric_query_col_name = sys.argv[5]
col_names_to_keep = sys.argv[6]

rel = duckdb.from_csv_auto(in_file_path)

if query_type == "simple":
    rel = rel.filter(f"({discrete_query_col_name} == 'AM' or {discrete_query_col_name} == 'NZ') and {numeric_query_col_name} >= 0.1").project(col_names_to_keep)
elif query_type == "startsendswith":
    discrete_values_A = ",".join([f"'A{x}'" for x in string.ascii_uppercase])
    discrete_values_Z = ",".join([f"'{x}Z'" for x in string.ascii_uppercase])
    rel = rel.filter(f"({discrete_query_col_name} in ({discrete_values_A}) or {discrete_query_col_name} in ({discrete_values_Z})) and {numeric_query_col_name} >= 0.1").project(col_names_to_keep)

with open(out_file_path, "w") as out_file:
    out_file.write("\t".join(col_names_to_keep.split(",")) + "\n")

    for row in rel.fetchall():
        out_file.write("\t".join([str(x) for x in row]) + "\n")
