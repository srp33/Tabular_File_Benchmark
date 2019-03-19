library(data.table)
library(readr)

in_file_path = commandArgs()[7]
col_names_file_path = commandArgs()[8]
out_file_path = commandArgs()[9]

column_indices = fread(col_names_file_path, sep="\t", header=FALSE, select=1, data.table=FALSE)[,1] + 1

data = fread(in_file_path, sep="\t", header=TRUE, stringsAsFactors=FALSE, select=column_indices, data.table=FALSE)

write_tsv(data, out_file_path)
