suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(vroom))

args = commandArgs(trailingOnly=TRUE)

settings = strsplit(args[1], ",")[[1]]

num_threads = 1
if (settings[1] == "8_threads") {
    num_threads = 8
}

altrep = FALSE
if (settings[2] == "altrep") {
    altrep = TRUE
}

query_type = args[2]
in_file_path = args[3]
out_file_path = args[4]
discrete_query_col_name = args[5]
numeric_query_col_name = args[6]
col_names_to_keep = strsplit(args[7], ",")[[1]]

data = suppressWarnings(suppressMessages(vroom(in_file_path, col_select=all_of(c(discrete_query_col_name, numeric_query_col_name, col_names_to_keep)), num_threads=num_threads, altrep = altrep)))

if (query_type == "simple") {
    output = filter(data, (!!sym(discrete_query_col_name)) %in% c('AM', 'NZ') & (!!sym(numeric_query_col_name) >= 0.1)) %>%
        select(all_of(col_names_to_keep)) %>%
        vroom_write(out_file_path)
} else {
    if (query_type == "startsendswith") {
        filter(data, (grepl("A\\w", (!!sym(discrete_query_col_name))) | grepl("\\wZ", (!!sym(discrete_query_col_name)))) & (!!sym(numeric_query_col_name) >= 0.1)) %>%
            select(all_of(col_names_to_keep)) %>%
            vroom_write(out_file_path)
    }
}
