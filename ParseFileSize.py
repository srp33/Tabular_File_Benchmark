import math
import os
import sys

file_path = sys.argv[1]

if os.path.exists(file_path):
    print(f"\t{math.ceil(os.path.getsize(file_path) / 1024)}", end="")
else:
    print(f"\tError", end="")
