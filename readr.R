suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(readr))

args = commandArgs(trailingOnly=TRUE)

settings = strsplit(args[1], ",")[[1]]

num_threads = 1
if (settings[1] == "8_threads") {
    num_threads = 8
}

lazy = FALSE
if (settings[2] == "lazy") {
    lazy = TRUE
}

query_type = args[2]
in_file_path = args[3]
out_file_path = args[4]
discrete_query_col_name = args[5]
numeric_query_col_name = args[6]
col_names_to_keep = args[7]

if (col_names_to_keep == "all_columns") {
    data = suppressWarnings(suppressMessages(read_tsv(in_file_path, num_threads=num_threads, lazy=lazy)))
} else {
    col_names_to_keep2 = strsplit(col_names_to_keep, ",")[[1]]
    data = suppressWarnings(suppressMessages(read_tsv(in_file_path, col_select=all_of(c(discrete_query_col_name, numeric_query_col_name, col_names_to_keep2)), num_threads=num_threads, lazy=lazy)))
}

if (query_type == "simple") {
    output = filter(data, (!!sym(discrete_query_col_name)) %in% c('AM', 'NZ') & (!!sym(numeric_query_col_name) >= 0.1))

    if (col_names_to_keep != "all_columns") {
        output = select(output, all_of(col_names_to_keep2))
    }
} else {
    if (query_type == "startsendswith") {
        output = filter(data, (grepl("A\\w", (!!sym(discrete_query_col_name))) | grepl("\\wZ", (!!sym(discrete_query_col_name)))) & (!!sym(numeric_query_col_name) >= 0.1))

        if (col_names_to_keep != "all_columns") {
            output = select(output, all_of(col_names_to_keep2))
        }
    }
}

write_tsv(output, out_file_path)
