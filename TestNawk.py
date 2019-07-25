import sys
import os
from Helper import *

file_path = sys.argv[1]
col_names_file_path = sys.argv[2]
out_file_path = sys.argv[3]

col_indices = [str(i + 1) for i in getColIndicesToQuery(col_names_file_path, True)]

command = "nawk -v OFS='\\t' ' {print " + "$" + ", $".join(col_indices) + ";}' " + file_path + " > " + out_file_path

with open(out_file_path, 'w') as out_file:
    out_file.write(command)

os.system(command)
