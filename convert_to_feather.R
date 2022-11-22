library(feather)

in_file_path = commandArgs(trailingOnly=TRUE)[1]
out_file_path = commandArgs(trailingOnly=TRUE)[2]

print(paste0("Reading from ", in_file_path))

data = read.table(in_file_path, header=TRUE, sep="\t", stringsAsFactors=FALSE, row.names=NULL, quote="", check.names=FALSE)

print(paste0("Writing to ", out_file_path))
write_feather(data, out_file_path)
