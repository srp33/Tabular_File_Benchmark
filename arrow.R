suppressPackageStartupMessages(library(arrow))

args = commandArgs(trailingOnly=TRUE)

file_format = args[1]
query_type = args[2]
in_file_path = args[3]
out_file_path = args[4]
discrete_query_col_name = args[5]
numeric_query_col_name = args[6]
col_names_to_keep = strsplit(args[7], ",")[[1]]

if (file_format == "feather2") {
  data = read_feather(in_file_path, col_select = all_of(c(discrete_query_col_name, numeric_query_col_name, col_names_to_keep)))
} else {
  data = read_parquet(in_file_path, col_select = all_of(c(discrete_query_col_name, numeric_query_col_name, col_names_to_keep)))
}

numeric_indices = which(data[,numeric_query_col_name] >= 0.1)

if (query_type == "simple") {
    discrete_indices = which(data[[discrete_query_col_name]] %in% c('AM', 'NZ'))
} else {
    if (query_type == "startsendswith") {
        discrete_values = data[[discrete_query_col_name]]
        discrete_indices = which(grepl("A\\w", discrete_values) | grepl("\\wZ", discrete_values))
    }
}

row_indices = intersect(discrete_indices, numeric_indices)

write.table(data[row_indices, col_names_to_keep], out_file_path, sep="\t", col.names=TRUE, row.names=FALSE, quote=FALSE)
