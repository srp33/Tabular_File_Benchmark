import sys

in_file_path = sys.argv[1]
out_file_path = sys.argv[2]

with open(in_file_path, "rb") as in_file:
    header_items = in_file.readline().rstrip(b"\n").split(b"\t")

    indices = [i for i in range(0, len(header_items), 100)]
    columns = [header_items[i] for i in indices]

with open(out_file_path, "wb") as out_file:
    for i in range(len(indices)):
        out_file.write("{}\t{}\n".format(indices[i], columns[i].decode()).encode())
