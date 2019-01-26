import msgpack
import os
import random
import string
import sys

dimensions = int(sys.argv[1])
out_file_path = sys.argv[2]

random.seed(0)

acgt = ["A", "C", "G", "T"]

num_homo_ref = int(float(dimensions) * 0.7)
num_het = int(float(dimensions) * 0.2)
num_homo_alt = int(float(dimensions) * 0.1)

genotype_options = []
for ref_allele in acgt:
    for alt_allele in acgt:
        if ref_allele == alt_allele:
            continue

        homo_ref_gt = ref_allele * 2
        het_gt = ref_allele + alt_allele
        homo_alt_gt = alt_allele * 2

        genotype_options.append((homo_ref_gt, het_gt, homo_alt_gt))

num_genotype_options = len(genotype_options)

def buildRows():
    for row_num in range(dimensions):
        genotype_option = genotype_options[random.randint(0, num_genotype_options - 1)]
        genotypes = (genotype_option[random.randint(0, 2)] for i in range(dimensions))

        yield "".join(genotypes) + "\n"

column_coord_dict = {}

cum_column_start = 0

for i in range(dimensions):
    column_coord_dict[i] = "{},{}".format(cum_column_start, 2)
    cum_column_start += 2

with open(out_file_path, 'wb') as out_file:
    cum_position = 0

    output = ""

    row_count = 0
    chunk_size = 1000

    for row in buildRows():
        output += row

        cum_position += len(row)

        if row_count % chunk_size == 0:
            out_file.write(output.encode())
            output = ""

        row_count += 1

    if len(output) > 0:
        out_file.write(output.encode())

# Save value that indicates line length
with open(out_file_path + ".ll", 'wb') as ll_file:
    line_length = dimensions * 2
    ll_file.write(str(line_length + 1).encode())

# Save value that indicates maximum length of column coords string
max_column_coord_length = max([len(x) for x in set(column_coord_dict.values())])
with open(out_file_path + ".mccl", 'wb') as mccl_file:
    # Add one to account for newline character
    mccl_file.write(str(max_column_coord_length + 1).encode())

# Save column coords
with open(out_file_path + ".cc", 'wb') as cc_file:
    formatter = "{:<" + str(max_column_coord_length) + "}\n"
    for key, value in sorted(column_coord_dict.items()):
        cc_file.write(formatter.format(value).encode())
