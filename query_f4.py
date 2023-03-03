import f4
import operator
import sys

num_processes = int(sys.argv[1])
query_type = sys.argv[2]
in_file_path = sys.argv[3]
out_file_path = sys.argv[4]
discrete_query_col_name = sys.argv[5]
numeric_query_col_name = sys.argv[6]
col_names_to_keep = sys.argv[7]

if col_names_to_keep == "all_columns":
    col_names_to_keep = None
else:
    col_names_to_keep = col_names_to_keep.split(",")

if query_type == "simple":
    disc_fltr1 = f4.StringFilter(discrete_query_col_name, operator.eq, "AM")
    disc_fltr2 = f4.StringFilter(discrete_query_col_name, operator.eq, "NZ")
    disc_fltr = f4.OrFilter(disc_fltr1, disc_fltr2)

    num_fltr = f4.FloatFilter(numeric_query_col_name, operator.ge, 0.1)

    fltr = f4.AndFilter(disc_fltr, num_fltr)

f4.Parser(in_file_path).query_and_write(fltr, col_names_to_keep, out_file_path = out_file_path, num_processes = num_processes)
