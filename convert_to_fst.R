suppressPackageStartupMessages(library(fst))

in_file_path = commandArgs(trailingOnly=TRUE)[1]
out_file_path = commandArgs(trailingOnly=TRUE)[2]

#data = suppressMessages(suppressWarnings(read_tsv(in_file_path)))

#data = suppressMessages(suppressWarnings(fread(in_file_path, sep="\t", header=TRUE, stringsAsFactors=FALSE, check.names=FALSE, data.table=FALSE, encoding="UTF-8")))
# *** caught segfault ***
#address 0x7f756f97aca3, cause 'memory not mapped'

data = read.table(in_file_path, header=TRUE, sep="\t", stringsAsFactors=FALSE, row.names=NULL, quote="", check.names=FALSE)

write.fst(data, out_file_path, compress=0)
