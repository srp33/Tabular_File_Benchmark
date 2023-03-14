import f4
import sys

tsv_file_path = sys.argv[1]
num_threads = int(sys.argv[2])
compression_type = None if sys.argv[3] == "None" else sys.argv[3]
index_columns = sys.argv[4].split(",")
out_file_path = sys.argv[5]

f4.convert_delimited_file(tsv_file_path, out_file_path, num_threads=num_threads, compression_type=compression_type, index_columns=index_columns)
f4.build_endswith_index(out_file_path, index_columns[0], "/tmp/custom_index", verbose=False)
