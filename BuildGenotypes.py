import msgpack
import os
import random
import string
import sys

dimensions = int(sys.argv[1])
out_file_path = sys.argv[2]

name_chars = string.digits + string.ascii_letters

# I am sure there is a better way to code these functions.
# But I can't think of it right now.
def getColRowNames(num):
    names = []

    if len(names) < num:
        names.extend(buildLetterPairs())
    if len(names) < num:
        names.extend(buildLetterTrios())
    if len(names) < num:
        names.extend(buildLetterQuads())

    return names[:num]

def buildLetterPairs():
    pairs = []

    for i in range(len(name_chars)):
        for j in range(len(name_chars)):
            pairs.append(name_chars[i] + name_chars[j])

    return pairs

def buildLetterTrios():
    trios = []

    for i in range(len(name_chars)):
        for j in range(len(name_chars)):
            for k in range(len(name_chars)):
                trios.append(name_chars[i] + name_chars[j] + name_chars[k])

    return trios

def buildLetterQuads():
    quads = []

    for i in range(len(name_chars)):
        for j in range(len(name_chars)):
            for k in range(len(name_chars)):
                for l in range(len(name_chars)):
                    quads.append(name_chars[i] + name_chars[j] + name_chars[k] + name_chars[l])

    return quads

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
        genotypes = [names[row_num]] + [random.choice(genotype_option) for i in range(dimensions)]

        yield convertListToFixedWidth(genotypes)

def convertListToFixedWidth(line_items):
    return "".join(column_format_dict[i].format(line_items[i]) for i in range(len(line_items)))

names = getColRowNames(dimensions)

column_format_dict = {}
column_coord_dict = {}
row_start_dict = {}

first_col_name = "Genotype"
max_length_first_col = max([len(first_col_name)] + [len(x) for x in names])
column_format_dict[0] = "{:<" + str(max_length_first_col) + "}"
column_coord_dict[0] = (0, max_length_first_col)

cum_column_start = max_length_first_col

for i in range(len(names)):
    column_name = names[i]

    column_format_dict[i+1] = "{:<" + str(len(column_name)) + "}"
    column_coord_dict[i+1] = (cum_column_start, cum_column_start + len(column_name))
    cum_column_start += len(column_name)

with open(out_file_path, 'wb') as out_file:
    cum_position = 0

    out_header_line = convertListToFixedWidth([first_col_name] + names)
    out_file.write(out_header_line.encode())

    row_start_dict[0] = cum_position
    cum_position += len(out_header_line)

    output = ""

    row_count = 1
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
