import f4
import sys

tsv_file_path = sys.argv[1]
num_discrete_vars = int(sys.argv[2])
num_continuous_vars = int(sys.argv[3])
num_rows = int(sys.argv[4])
#index_columns = sys.argv[]
#comment_prefix = sys.argv[]
#compression_type = sys.argv[]
num_processes = int(sys.argv[5])
#tmp_dir_path
out_file_path = sys.argv[6]

num_cols_per_chunk = 101
num_rows_per_write = 25001
if num_rows == 1000:
    num_cols_per_chunk = 25001
    num_rows_per_write = 101

#f4.convert_delimited_file(tsv_file_path, out_file_path, index_columns=[], delimiter="\t", comment_prefix="#", compression_type=None, num_processes=1, num_cols_per_chunk=100, num_rows_per_write=100, tmp_dir_path=None, verbose=False)
f4.convert_delimited_file(tsv_file_path, out_file_path, num_processes=num_processes, num_cols_per_chunk=num_cols_per_chunk, num_rows_per_write=num_rows_per_write, verbose=False)
