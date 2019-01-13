import os
import sys

file_path = sys.argv[1]
compression_method = sys.argv[2]
compression_level = sys.argv[3]

print("Compressing {} with method {} and level {}".format(file_path, compression_method, compression_level))

if compression_method == "bz2":
    import bz2 as cmpr
    compression_code = "cmpr.compress(line, compresslevel={})".format(compression_level)
    out_file_path = file_path + "." + compression_method + "_" + compression_level
elif compression_method == "gz":
    import gzip as cmpr
    compression_code = "cmpr.compress(line, compresslevel={})".format(compression_level)
    out_file_path = file_path + "." + compression_method + "_" + compression_level
elif compression_method == "lzma":
    import lzma as cmpr
    compression_code = "cmpr.compress(line)"
    out_file_path = file_path + "." + compression_method
elif compression_method == "snappy":
    import snappy as cmpr
    compression_code = "cmpr.compress(line)"
    out_file_path = file_path + "." + compression_method
else:
    print("No matching compression method")
    sys.exit(1)

with open(file_path, 'rb') as my_file:
    with open(out_file_path, 'wb') as out_file:
        for line in my_file:
            line = line.rstrip(b"\n")

            compressed_line = eval(compression_code)

            out_file.write(compressed_line + b"\n")
