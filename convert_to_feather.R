suppressPackageStartupMessages(library(feather))

in_file_path = commandArgs(trailingOnly=TRUE)[1]
out_file_path = commandArgs(trailingOnly=TRUE)[2]

data = read.table(in_file_path, header=TRUE, sep="\t", stringsAsFactors=FALSE, row.names=NULL, quote="", check.names=FALSE)

write_feather(data, out_file_path)
