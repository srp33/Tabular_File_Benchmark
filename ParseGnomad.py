import copy
import gzip
import re
import sys

gnomad_vcf_file_path = sys.argv[1]
out_file_path = sys.argv[2]

info_items = list()
info_types = {}
comment_lines = 0
header_items = None

# Parse metadata values and header first
with gzip.open(gnomad_vcf_file_path) as gnomad_file:
    for line in gnomad_file:
        comment_lines += 1
        line = line.decode().rstrip("\n")

        if line.startswith("##"):
            if line.startswith("##INFO=<ID="):
                x = line.replace("##INFO=<ID=", "").split(",")

                # We ignore vep annotations for simplicity
                if x[0] != "vep":
                    info_items.append(x[0])
                    info_types[x[0]] = x[2].replace("Type=", "")
        elif line.startswith("#"):
            header_items = line.split("\t")
            header_items[0] = header_items[0][1:]
        else:
            break

with gzip.open(out_file_path, 'w') as out_file:
    out_file.write(("\t".join(header_items[:-1] + info_items) + "\n").encode())

    with gzip.open(gnomad_vcf_file_path) as gnomad_file:
        # Advance to the end of the comment lines
        for i in range(comment_lines):
            gnomad_file.readline()

        line_count = 0
        for line in gnomad_file:
            line = line.decode().rstrip("\n")

            line_count += 1
            if line_count % 100000 == 0:
                print("Processed {} lines - {}".format(line_count, line[:20]), flush=True)
                break

            line_items = line.split("\t")
            out_items = line_items[:-1]
            # Remove unnecessary decimal places
            out_items[5] = out_items[5].replace(".00", "")

            item_dict = {}
            for item in line_items[-1].split(";")[:-1]:
                item_split = item.split("=")
                item_key = item_split[0]

                if info_types[item_key] == "Flag":
                    item_dict[item_key] = "1"
                else:
                    item_value = item_split[1]

                    # Shorten some of the float representations
                    if info_types[item_key] == "Float":
                        if item_value != ".":
                            if item_value == "0.00000e+00":
                                item_value = "0"
                            else:
                                item_value = re.sub(r"0+e", "e", item_value).replace("e+00", "")
                    item_dict[item_key] = item_value

            for info_item in info_items:
                if info_item in item_dict:
                    out_items.append(item_dict[info_item])
                else:
                    out_items.append("")

            out_file.write(("\t".join(out_items) + "\n").encode())
