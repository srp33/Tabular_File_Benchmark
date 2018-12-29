import os
import random
import string
import sys
import msgpack
import snappy

num_continuous_vars = int(sys.argv[1])
num_discrete_vars = int(sys.argv[2])
num_rows = int(sys.argv[3])
out_file_path = sys.argv[4]

random.seed(0)

letters = string.ascii_letters[26:]

with open(out_file_path, 'wb') as out_file:
    number_col_names = ["Numeric{}".format(i+1) for i in range(num_continuous_vars)]
    discrete_col_names = ["Discrete{}".format(i+1) for i in range(num_discrete_vars)]
    out_file.write(msgpack.packb(number_col_names + discrete_col_names))

    for row_num in range(num_rows):
        numbers = ["{:.8f}".format(random.random()) for i in range(num_continuous_vars)]
        discrete = [random.choice(letters) + random.choice(letters) for i in range(num_discrete_vars)]

        out_file.write(msgpack.packb(numbers + discrete))

        #if row_num > 0 and row_num % 1000 == 0:
            #print(row_num)
            #sys.stdout.flush()
