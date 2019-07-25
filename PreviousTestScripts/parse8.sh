#!/bin/bash

instInfoFileName=tmp/GSE92742_Broad_LINCS_inst_info.txt.gz
cellInfo=tmp/GSE92742_Broad_LINCS_cell_info.txt.gz
pertInfo=tmp/GSE92742_Broad_LINCS_pert_info.txt.gz	
pertMetrics=tmp/GSE92742_Broad_LINCS_pert_metrics.txt.gz
gctxFileName=tmp/LINCS_PhaseI_Level3.gctx
geneFile=tmp/GSE92742_Broad_LINCS_gene_info.txt.gz

#python ParseMetadata.py $instInfoFileName $cellInfo $pertInfo $pertMetrics tmp/Metadata.tsv.gz
#python convertTallFormatToWide.py tmp/Metadata.tsv.gz Metadata.tsv.gz
##python3 ParseMetadata.py $instInfoFileName $cellInfo $pertInfo $pertMetrics tmp/Metadata.tsv.gz
#python3 convertTallFormatToWide.py tmp/Metadata.tsv.gz Metadata.tsv.gz



time python3 BuildRandomAccessMP.py /Applications/tmp/Metadata.tsv.gz /Applications/tmp/Metadata.mp.gz
#time python3 BuildRandomAccessMP.py /Applications/tmp/Gene_Expression.tsv.gz /Applications/tmp/Gene_Expression.mp.gz

#time python3 RandomlyAccessFile.py /Applications/tmp/Metadata.mp.gz /Applications/tmp/Metadata.tsv.gz /Applications/tmp/Metadata_temp.tsv.gz --num_samples=1000
#time python3 RandomlyAccessFile.py /Applications/tmp/Gene_Expression.mp.gz /Applications/tmp/Gene_Expression.tsv.gz /Applications/tmp/Gene_Expression_temp.tsv.gz --num_samples=100000

#time python3 TransposeTSV.py /Applications/tmp/Metadata.tsv.gz /Applications/tmp/Metadata.mp.gz /Applications/tmp/Metadata_transposed.tsv.gz --temp_dir=/Applications/tmp/Metadata_transpose.gz_tmp --num_data_points=5000000 --gzip
#time python3 TransposeTSV.py /Applications/tmp/Gene_Expression.tsv.gz /Applications/tmp/Gene_Expression.mp.gz /Applications/tmp/Gene_Expression_transposed.tsv.gz --temp_dir=/Applications/tmp/Gene_Expression_transpose.gz_tmp

#time python3 BuildRandomAccessMP.py /Applications/tmp/Metadata_transposed.tsv.gz /Applications/tmp/Metadata_transposed.mp.gz
#time python3 BuildRandomAccessMP.py /Applications/tmp/Gene_Expression_transposed.tsv.gz /Applications/tmp/Gene_Expression_transposed.mp.gz
