suppressMessages(library(feather))
suppressMessages(library(readr))
suppressMessages(library(dplyr))
#~0.31 seconds to here

in_file_path = commandArgs()[7]
col_names_file_path = commandArgs()[8]
out_file_path = commandArgs()[9]
discrete_query_col_index = as.integer(commandArgs()[10]) + 1
num_query_col_index = as.integer(commandArgs()[11]) + 1

select_column_names = as.data.frame(suppressMessages(read_tsv(col_names_file_path, col_names=FALSE)))[,2]
#~0.33 seconds to here

######################################################
# The first way I did this was really slow.
# This is shown in the commented portions below.
######################################################

#  suppressMessages(read_feather(in_file_path)) %>%
#    filter((grepl("^A.+", .[[discrete_query_col_index]]) | grepl(".+Z$", .[[discrete_query_col_index]])) & .[[num_query_col_index]] >= 0.1) %>%
#    select(select_column_names) %>%
#    write_tsv(out_file_path)

suppressMessages(read_feather(in_file_path, columns=c(discrete_query_col_index, num_query_col_index))) %>%
  mutate(row_num=row_number()) %>%
  filter((grepl("^A.+", .[[1]]) | grepl(".+Z$", .[[1]])) & .[[2]] >= 0.1) %>%
  pull(row_num) -> matching_row_numbers
# 0.48 seconds

suppressMessages(read_feather(in_file_path, columns=select_column_names)) %>%
  mutate(row_num=row_number()) %>%
  filter(row_num %in% matching_row_numbers) %>%
  select(-row_num) %>%
  write_tsv(out_file_path)
# 1.23 seconds
