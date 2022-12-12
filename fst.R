suppressPackageStartupMessages(library(fst))

args = commandArgs(trailingOnly=TRUE)

query_type = args[1]
in_file_path = args[2]
out_file_path = args[3]
discrete_query_col_name = args[4]
numeric_query_col_name = args[5]
col_names_to_keep = args[6]

filter_rows = function(data, query_type, numeric_query_col_name, discrete_query_col_name) {
    numeric_indices = which(data[,numeric_query_col_name] >= 0.1)

    if (query_type == "simple") {
        discrete_indices = which(data[,discrete_query_col_name] %in% c('AM', 'NZ'))
    } else {
        if (query_type == "startsendswith") {
            discrete_indices = which(grepl("A\\w", data[,discrete_query_col_name]) | grepl("\\wZ", data[,discrete_query_col_name]))
        }
    }

    return(intersect(discrete_indices, numeric_indices))
}

if (col_names_to_keep == "all_columns") {
    data = read_fst(in_file_path, columns = c(discrete_query_col_name, numeric_query_col_name))
    row_indices = filter_rows(data, query_type, numeric_query_col_name, discrete_query_col_name)

    # This could be optimized further by looking for consecutive row indices,
    # but it would likely not overcome the performance problems.
    output = read_fst(in_file_path, from = row_indices[1], to = row_indices[1])
    write.table(output, out_file_path, sep="\t", col.names=TRUE, row.names=FALSE, quote=FALSE)

    for (i in row_indices[2:length(row_indices)]) {
        row = read_fst(in_file_path, from = i, to = i)
        write.table(row, out_file_path, sep="\t", col.names=FALSE, row.names=FALSE, quote=FALSE, append=TRUE)
    }
} else {
    col_names_to_keep2 = strsplit(col_names_to_keep, ",")[[1]]
    data = read_fst(in_file_path, columns = c(discrete_query_col_name, numeric_query_col_name, col_names_to_keep2))
    row_indices = filter_rows(data, query_type, numeric_query_col_name, discrete_query_col_name)

    output = data[row_indices, col_names_to_keep2]

    # This way is at least twice as slow.
    #col_names_to_keep2 = strsplit(col_names_to_keep, ",")[[1]]
    #data = read_fst(in_file_path, columns = c(discrete_query_col_name, numeric_query_col_name))
    #row_indices = filter_rows(data, query_type, numeric_query_col_name, discrete_query_col_name)
    #output = NULL
    #for (i in row_indices) {
    #    output = rbind(output, read_fst(in_file_path, columns = col_names_to_keep2, from = i, to = i))
    #}

    write.table(output, out_file_path, sep="\t", col.names=TRUE, row.names=FALSE, quote=FALSE)
}
