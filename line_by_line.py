import mmap
import sys
import fastnumbers

settings = sys.argv[1].split(",")
query_type = sys.argv[2]
in_file_path = sys.argv[3]
out_file_path = sys.argv[4]
discrete_query_col_name = sys.argv[5].encode()
numeric_query_col_name = sys.argv[6].encode()
col_names_to_keep = sys.argv[7]

with open(in_file_path, 'rb') as my_file:
    if settings[0] == "memory_map":
        my_file = mmap.mmap(my_file.fileno(), 0, prot=mmap.PROT_READ)

    with open(out_file_path, 'wb') as out_file:
        header_line = my_file.readline()
        header_items = header_line.rstrip(b"\n").split(b"\t")
        discrete_query_col_index = header_items.index(discrete_query_col_name)
        numeric_query_col_index = header_items.index(numeric_query_col_name)

        if col_names_to_keep == "all_columns":
            col_indices_to_keep = list(range(len(header_items)))
            out_file.write(header_line)
        else:
            col_names_to_keep = [name.encode() for name in col_names_to_keep.split(",")]
            col_indices_to_keep = [header_items.index(name) for name in col_names_to_keep]
            out_file.write(b"\t".join(col_names_to_keep) + b"\n")

        for line in iter(my_file.readline, b""):
            line_items = line.rstrip(b"\n").split(b"\t")

            discrete_value = line_items[discrete_query_col_index]
            num_value = fastnumbers.float(line_items[numeric_query_col_index])

            if query_type == "simple":
                if (discrete_value == b"AM" or discrete_value == b"NZ") and num_value >= 0.1:
                    out_file.write(b"\t".join([line_items[i] for i in col_indices_to_keep]) + b"\n")
            elif query_type == "startsendswith":
                if (discrete_value.startswith(b"A") or discrete_value.endswith(b"Z")) and num_value >= 0.1:
                    out_file.write(b"\t".join([line_items[i] for i in col_indices_to_keep]) + b"\n")

    my_file.close()
