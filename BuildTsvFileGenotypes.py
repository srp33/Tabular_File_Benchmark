import os
import random
import sys

dimensions = int(sys.argv[1])
out_file_path = sys.argv[2]

random.seed(0)

acgt = ["A", "C", "G", "T"]

with open(out_file_path, 'wb') as out_file:
    col_names = ["C{}".format(i+1) for i in range(dimensions)]
    out_file.write(("\t".join(["Genotype"] + col_names) + "\n").encode())

    output = ""

    for row_num in range(dimensions):
        ref_allele = random.choice(acgt)
        alt_allele = random.choice([x for x in acgt if x != ref_allele])

        genotypes = []
        for i in range(int(float(dimensions) * 0.5)):
            genotypes.append(ref_allele + ref_allele)
        for i in range(int(float(dimensions) * 0.3)):
            genotypes.append(ref_allele + alt_allele)
        for i in range(int(float(dimensions) * 0.2)):
            genotypes.append(alt_allele + alt_allele)

        random.shuffle(genotypes)

        output += "\t".join(["R{}".format(row_num + 1)] + genotypes) + "\n"

        if row_num > 0 and row_num % 10000 == 0:
            print(row_num, flush=True)
            out_file.write(output.encode())
            output = ""

    if len(output) > 0:
        out_file.write(output.encode())
