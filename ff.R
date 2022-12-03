suppressPackageStartupMessages(library(ff))

args = commandArgs(trailingOnly=TRUE)

query_type = args[1]
in_file_path = args[2]
out_file_path = args[3]
discrete_query_col_name = args[4]
numeric_query_col_name = args[5]
col_names_to_keep = args[6]

data = read.table.ffdf(file=in_file_path, header=TRUE, check.names=FALSE)

if (col_names_to_keep != "all_columns") {
    col_names_to_keep2 = strsplit(col_names_to_keep, ",")[[1]]
}

numeric_indices = which(data[,numeric_query_col_name] >= 0.1)

if (query_type == "simple") {
    discrete_indices = which(data[,discrete_query_col_name] %in% c('AM', 'NZ'))
} else {
    if (query_type == "startsendswith") {
        discrete_indices = which(grepl("A\\w", data[,discrete_query_col_name]) | grepl("\\wZ", data[,discrete_query_col_name]))
    }
}

row_indices = intersect(discrete_indices, numeric_indices)

if (col_names_to_keep == "all_columns") {
    output = as.ffdf(data[row_indices,])
} else {
    output = as.ffdf(data[row_indices, col_names_to_keep2])
}

write.table.ffdf(output, file=out_file_path, sep="\t", col.names=TRUE, row.names=FALSE, quote=FALSE)
