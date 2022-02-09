import os
import sys

file_path = sys.argv[1]

extensions = [".rowstart", ".cc", ".ct", ".ll", ".mccl", ".mctl", ".mrsl"]

total = os.path.getsize(file_path)

for ext in extensions:
    f = file_path + ext

    if os.path.exists(f):
        total += os.path.getsize(f)

print(total)
