import f4
import sys

in_file_path = sys.argv[1]
out_file_path = sys.argv[2]

f4.convert_delimited_file(in_file_path, out_file_path, comment_prefix="##", compression_type="zstd", num_parallel=16, tmp_dir_path="/tmp/convert_cadd", verbose=True)
#f4.head(out_file_path)
#index_columns=[["#Chrom", "Pos"], "Type", "AnnoType", "Consequence", "PHRED"],
