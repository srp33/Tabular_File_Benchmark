import sys

for line in sys.stdin:
    line_items = line.rstrip("\n").split(": ")

    if line_items[0].strip() == "Elapsed (wall clock) time (h:mm:ss or m:ss)":
        time = line_items[1]
        time_items = time.split(":")

        hours = 0
        if len(time_items) == 3:
            hours = int(time_items[0])
        minutes = int(time_items[-2])
        seconds = float(time_items[-1])

        seconds = seconds + minutes * 60 + hours * 3600

    if line_items[0].strip() == "Maximum resident set size (kbytes)":
        memory = line_items[1]

print(f"{seconds}\t{memory}")
