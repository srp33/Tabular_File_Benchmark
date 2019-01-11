import bz2
import gzip
import lzma
import os
import snappy
import sys

file_path = sys.argv[1]
compression_method = sys.argv[2]

out_file_path = file_path + "." + compression_method

if os.path.exists(out_file_path):
    print("{} already exists".format(out_file_path))
    sys.exit(0)

with open(file_path, 'rb') as my_file:
    with open(out_file_path, 'wb') as out_file:
        line_count = 0
        for line in my_file:
            line_count += 1

            if line_count % 1000 == 0:
                print(line_count, flush=True)

            line = line.rstrip(b"\n")

            if compression_method == "bz2":
                compressed_line = bz2.compress(line)
            elif compression_method == "gz":
                compressed_line = gzip.compress(line)
            elif compression_method == "lzma":
                compressed_line = lzma.compress(line)
            elif compression_method == "snappy":
                compressed_line = snappy.compress(line)
            else:
                print("No matching compression method")
                sys.exit(1)

            out_file.write(compressed_line + b"\n")
