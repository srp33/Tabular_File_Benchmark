import sys

for line in sys.stdin:
    line_items = line.rstrip("\n").split(": ")

    if line_items[0].strip() == "Maximum resident set size (kbytes)":
        print(line_items[1])
