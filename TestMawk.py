import sys
import os
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]

with open(file_path, 'r') as my_file:
    header_items = next(my_file).rstrip("\n").split("\t")
    indices = [str(i) for i in range(1, len(header_items) + 1, 100)]

command = "mawk -v OFS='\\t' ' {print " + "$" + ", $".join(indices) + ";}' " + file_path + " > " + out_file_path

with open(out_file_path, 'w') as out_file:
    out_file.write(command)

os.system(command)
