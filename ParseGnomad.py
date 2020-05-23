import copy
import gzip
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

            # Create default dictionary
            item_dict = {}
            for info_item in info_items:
                if info_types[info_item] == "Flag":
                    item_dict[info_item] = "0"
                else:
                    item_dict[info_item] = ""

            # We skip the last item (vep)
            for item in line_items[-1].split(";")[:-1]:
                item_split = item.split("=")
                item_key = item_split[0]
                item_split.append("1")
                item_dict[item_key] = item_split[1]

            for info_item in info_items:
                out_items.append(item_dict[info_item])

            out_file.write(("\t".join(out_items) + "\n").encode())
