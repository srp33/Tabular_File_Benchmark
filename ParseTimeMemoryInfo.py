import sys

in_file_path = sys.argv[1]

with open(in_file_path) as in_file:
    for line in in_file:
        line_items = line.rstrip("\n").split(": ")

        if line_items[0].strip() == "Elapsed (wall clock) time (h:mm:ss or m:ss)":
            time = line_items[1]
            time_items = time.split(":")

            hours = 0
            if len(time_items) == 3:
                hours = int(time_items[0])
            minutes = int(time_items[-2])
            seconds = float(time_items[-1])

            wall_seconds = seconds + minutes * 60 + hours * 3600

        if line_items[0].strip() == "Maximum resident set size (kbytes)":
            memory = line_items[1]

        if line_items[0].strip() == "User time (seconds)":
            user_seconds = line_items[1]
        if line_items[0].strip() == "System time (seconds)":
            system_seconds = line_items[1]

print(f"{wall_seconds}\t{user_seconds}\t{system_seconds}\t{memory}", end="")
