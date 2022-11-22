library(feather)
library(fst)

in_file_path = commandArgs()[7]
out_feather_file_path = commandArgs()[8]
out_fst_file_path = commandArgs()[9]

print(paste0("Reading from ", in_file_path))
# readr and data.table did not work with wide files
#data = suppressMessages(suppressWarnings(read_tsv(in_file_path)))
data = suppressMessages(suppressWarnings(fread(in_file_path, sep="\t", header=TRUE, stringsAsFactors=FALSE, check.names=FALSE, data.table=FALSE, encoding="UTF-8")))
#data = read.table(in_file_path, header=TRUE, sep="\t", stringsAsFactors=FALSE, row.names=NULL, quote="", check.names=FALSE)

print(paste0("Writing to ", out_feather_file_path))
write_feather(data, out_feather_file_path)

print(paste0("Writing to ", out_fst_file_path))
write.fst(data, out_fst_file_path, compress=0)
