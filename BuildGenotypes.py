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

genotype_option_indices = range(len(genotype_options))

def buildRows():
    for row_num in range(dimensions):
        genotype_option_index = random.choice(genotype_option_indices)
        genotype_option = genotype_options[genotype_option_index]
        genotypes = (random.choice(genotype_option) for i in range(dimensions))

        yield "".join(genotypes) + "\n"

column_coord_dict = {}
row_start_dict = {}

cum_column_start = 0

for i in range(dimensions):
    column_coord_dict[i] = (cum_column_start, cum_column_start + 2)
    cum_column_start += 2

with open(out_file_path, 'wb') as out_file:
    cum_position = 0

    output = ""

    row_count = 0
    chunk_size = 100

    for row in buildRows():
        output += row

        row_start_dict[row_count] = cum_position
        cum_position += len(row)

        if row_count % chunk_size == 0:
            out_file.write(output.encode())
            output = ""

        row_count += 1

    if len(output) > 0:
        out_file.write(output.encode())

# Serialize and save dictionary that indicates where each row starts
with open(out_file_path + ".rowdict", 'wb') as rowdict_file:
    rowdict_file.write(msgpack.packb(row_start_dict, use_bin_type=True))

# Serialize and save dictionary that indicates where each column starts and ends
with open(out_file_path + ".coldict", 'wb') as coldict_file:
    coldict_file.write(msgpack.packb(column_coord_dict, use_bin_type=True))
