# https://stackoverflow.com/questions/1883980/find-the-nth-occurrence-of-substring-in-a-string
import mmap
import sys

file_path = sys.argv[1]
out_file_path = sys.argv[2]
memory_map = sys.argv[3] == "True"

with open(file_path, 'r') as my_file:
    header_items = my_file.readline().rstrip("\n").split("\t")
    index_range = range(0, len(header_items), 100)

with open(file_path, 'rb') as my_file:
    if memory_map:
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        for line in iter(my_file.readline, b""):
            out_items = []
            token_index = 0
            for i in index_range:
                token = "@{}@".format(i).encode()
                token_index = line.find(token, token_index)
                data_index = token_index + len(token)
                token_index = line.find(b"\t", data_index)
                if token_index == -1:
                    out_items.append(line[data_index:-1])
                else:
                    out_items.append(line[data_index:token_index])

            out_file.write(b"\t".join(out_items) + b"\n")

    my_file.close()
