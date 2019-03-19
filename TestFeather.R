suppressMessages(library(feather))
suppressMessages(library(readr))

in_file_path = commandArgs()[7]
col_names_file_path = commandArgs()[8]
out_file_path = commandArgs()[9]

column_names = as.data.frame(suppressMessages(read_tsv(col_names_file_path, col_names=FALSE)))[,2]

data = read_feather(in_file_path, columns = column_names)

write_tsv(data, out_file_path)
