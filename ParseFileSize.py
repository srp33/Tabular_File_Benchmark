import glob
import math
import os
import sys

file_pattern = sys.argv[1]

total_size = 0
file_paths = glob.glob(file_pattern)

if len(file_paths) == 0:
    print(f"\tError", end="")
else:
    for file_path in glob.glob(file_pattern):
        total_size += os.path.getsize(file_path)

    print(f"\t{math.ceil(total_size / 1024)}", end="")
