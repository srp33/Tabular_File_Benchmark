library(data.table)

args = commandArgs(trailingOnly=TRUE)

settings = strsplit(args[1], ",")[[1]]

num_threads = 1
if (settings[1] == "8_threads") {
    num_threads = 8
}

query_type = args[2]
in_file_path = args[3]
out_file_path = args[4]
discrete_query_col_name = args[5]
numeric_query_col_name = args[6]
col_names_to_keep = args[7]

if (col_names_to_keep == "all_columns") {
    data = fread(in_file_path, sep="\t", nThread=num_threads)
} else {
    col_names_to_keep2 = strsplit(col_names_to_keep, ",")[[1]]
    data = fread(in_file_path, select=c(discrete_query_col_name, numeric_query_col_name, col_names_to_keep2), sep="\t", nThread=num_threads)
}

numeric_indices = which(data[[numeric_query_col_name]] >= 0.1)

if (query_type == "simple") {
    discrete_indices = which(data[[discrete_query_col_name]] %in% c('AM', 'NZ'))
} else {
    discrete_values = data[[discrete_query_col_name]]
    discrete_indices = which(grepl("A\\w", discrete_values) | grepl("\\wZ", discrete_values))
}

row_indices = intersect(discrete_indices, numeric_indices)

if (col_names_to_keep == "all_columns") {
    output = data[row_indices,]
} else {
    output = data[row_indices, ..col_names_to_keep2]
}

fwrite(output, out_file_path, sep="\t", nThread=num_threads, quote=FALSE, na="NA")
