import f4
import operator
import sys

num_threads = int(sys.argv[1])
compression_type = None if sys.argv[2] == "None" else sys.argv[2]
query_type = sys.argv[3]
in_file_path = sys.argv[4]
out_file_path = sys.argv[5]
discrete_query_col_name = sys.argv[6]
numeric_query_col_name = sys.argv[7]
col_names_to_keep = sys.argv[8]

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
else:
    disc_fltr1 = f4.StartsWithFilter(discrete_query_col_name, "A")
    disc_fltr2 = f4.EndsWithFilter(discrete_query_col_name, "Z")
    disc_fltr = f4.OrFilter(disc_fltr1, disc_fltr2)

    num_fltr = f4.FloatFilter(numeric_query_col_name, operator.ge, 0.1)

    fltr = f4.AndFilter(disc_fltr, num_fltr)

f4.query(in_file_path, fltr, select_columns=col_names_to_keep, out_file_path = out_file_path, num_threads = num_threads)
