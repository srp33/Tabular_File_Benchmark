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
        line = line.decode().rstrip("\n")

        if line.startswith("##"):
            if line.startswith("##INFO=<ID="):
                x = line.replace("##INFO=<ID=", "").split(",")

                info_items.append(x[0])
                info_types[x[0]] = x[2].replace("Type=", "")
        elif line.startswith("#"):
            header_items = line.split("\t")
            header_items[0] = header_items[0][1:]
        else:
            break

        comment_lines += 1

with gzip.open(out_file_path, 'w') as out_file:
    out_file.write(("\t".join(header_items[:-1] + info_items) + "\n").encode())

    with gzip.open(gnomad_vcf_file_path) as gnomad_file:
        # Advance to the end of the comment lines
        for i in range(comment_lines):
            gnomad_file.readline()

        chunk_lines = []
        for line in gnomad_file:
            line = line.decode().rstrip("\n")
            line_items = line.split("\t")
            out_items = line_items[:-1]
            # Remove unnecessary decimal places from quality scores
            out_items[5] = out_items[5].replace(".00", "")

            item_dict = {}
            for item in line_items[-1].split(";"):
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
                    if info_types[info_item] == "Flag":
                        out_items.append("0")
                    else:
                        out_items.append("")

            chunk_lines.append("\t".join(out_items))
            if len(chunk_lines) % 100000 == 0:
                print("Reached {}".format(line[:15]), flush=True)
                out_file.write(("\n".join(chunk_lines) + "\n").encode())
                chunk_lines = []
                #break

        if len(chunk_lines) > 0:
            out_file.write(("\n".join(chunk_lines) + "\n").encode())
