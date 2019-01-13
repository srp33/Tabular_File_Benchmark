import os
import random
import string
import sys
import msgpack
import snappy

num_discrete_vars = int(sys.argv[1])
num_continuous_vars = int(sys.argv[2])
num_rows = int(sys.argv[3])
out_file_path = sys.argv[4]

random.seed(0)

letters = string.ascii_letters[26:]

with open(out_file_path, 'wb') as out_file:
    discrete_col_names = ["Discrete{}".format(i+1) for i in range(num_discrete_vars)]
    number_col_names = ["Numeric{}".format(i+1) for i in range(num_continuous_vars)]
    out_file.write(msgpack.packb(["ID"] + discrete_col_names + number_col_names))

    for row_num in range(num_rows):
        discrete = [random.choice(letters) + random.choice(letters) for i in range(num_discrete_vars)]
        numbers = ["{:.8f}".format(random.random()) for i in range(num_continuous_vars)]

        out_file.write(msgpack.packb(["Row" + str(row_num + 1)] + discrete + numbers))

        #if row_num > 0 and row_num % 1000 == 0:
            #print(row_num)
            #sys.stdout.flush()
