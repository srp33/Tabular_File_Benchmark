library(readr)

in_file_path = commandArgs()[7]
col_names_file_path = commandArgs()[8]
out_file_path = commandArgs()[9]

column_names = as.data.frame(suppressMessages(read_tsv(col_names_file_path, col_names=FALSE)))[,2]
col_types = paste0(rep("c", length(column_names)), collapse="")

# This is not really supported. You'd have to parse all the column names from the data file
# first (which is **really** slow) and then build a list from that.
data = suppressWarnings(suppressMessages(read_tsv(in_file_path, col_names=column_names)))
print(data)

write_tsv(data, out_file_path)
