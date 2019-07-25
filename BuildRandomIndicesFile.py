import random
import sys

dimensions = int(sys.argv[1])
seed = int(sys.argv[2])
out_file_path = sys.argv[3]

all_indices = list(range(dimensions))

random.seed(seed)
random.shuffle(all_indices)
indices = sorted(all_indices[:10])
#indices = sorted([random.randint(0, dimensions-1) for i in range(10)])

with open(out_file_path, "wb") as out_file:
    for i in range(len(indices)):
        out_file.write("{}\n".format(indices[i]).encode())
