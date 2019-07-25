#!/bin/bash

instInfoFileName=tmp/GSE92742_Broad_LINCS_inst_info.txt.gz
cellInfo=tmp/GSE92742_Broad_LINCS_cell_info.txt.gz
pertInfo=tmp/GSE92742_Broad_LINCS_pert_info.txt.gz	
pertMetrics=tmp/GSE92742_Broad_LINCS_pert_metrics.txt.gz
gctxFileName=tmp/LINCS_PhaseI_Level3.gctx
geneFile=tmp/GSE92742_Broad_LINCS_gene_info.txt.gz

#python ParseMetadata.py $instInfoFileName $cellInfo $pertInfo $pertMetrics tmp/Metadata.tsv.gz
#python convertTallFormatToWide.py tmp/Metadata.tsv.gz Metadata.tsv.gz

#python ParseExpression.py $gctxFileName $geneFile Gene_Expression.tsv.gz


#time python3 BuildRandomAccessMP.py /Applications/tmp/Metadata.tsv /Applications/tmp/Metadata.mp
#time python3 BuildRandomAccessMP.py /Applications/tmp/Gene_Expression.tsv /Applications/tmp/Gene_Expression.mp

#time python3 RandomlyAccessFile.py /Applications/tmp/Metadata.mp /Applications/tmp/Metadata.tsv /Applications/tmp/Metadata_temp.tsv --num_samples=1000
time python3 RandomlyAccessFile.py /Applications/tmp/Gene_Expression.mp /Applications/tmp/Gene_Expression.tsv /Applications/tmp/Gene_Expression_temp.tsv --num_samples=100000

#time python3 TransposeTSV.py /Applications/tmp/Metadata.tsv /Applications/tmp/Metadata.mp /Applications/tmp/Metadata_transposed.tsv --temp_dir=/Applications/tmp/Metadata_transpose_tmp --num_data_points=5000000
#time python3 TransposeTSV.py /Applications/tmp/Gene_Expression.tsv /Applications/tmp/Gene_Expression.mp /Applications/tmp/Gene_Expression_transposed.tsv --temp_dir=/Applications/tmp/Gene_Expression_transpose_tmp

#time python3 BuildRandomAccessMP.py /Applications/tmp/Metadata_transposed.tsv /Applications/tmp/Metadata_transposed.mp
#time python3 BuildRandomAccessMP.py /Applications/tmp/Gene_Expression_transposed.tsv /Applications/tmp/Gene_Expression_transposed.mp

tmpDir=/Applications/tmp

#####################################################
# Test it by removing a couple lines from Metadata.tsv, remapping it, and then merging, make sure NAs
#time python3 BuildRandomAccessMP.py $tmpDir/Metadata.tsv $tmpDir/Metadata.mp
#time python3 BuildRandomAccessMP.py $tmpDir/Metadata2.tsv $tmpDir/Metadata2.mp
#time python3 FileMerger.py --files $tmpDir/Metadata.tsv $tmpDir/Metadata2.tsv --mp_dirs $tmpDir/Metadata.mp $tmpDir/Metadata2.mp --prefixes Metadata Metadata2 --chunk_size 50000 --output_file $tmpDir/Metadata_Metadata2.tsv
#vim $tmpDir/Metadata_Metadata2.tsv
#####################################################

#time python3 FileMerger.py --files $tmpDir/Metadata.tsv $tmpDir/Gene_Expression.tsv --mp_dirs $tmpDir/Metadata.mp $tmpDir/Gene_Expression.mp --prefixes Metadata Gene_Expression --chunk_size 50000 --output_file $tmpDir/Metadata_Gene_Expression.tsv
#time python3 BuildRandomAccessMP.py /Applications/tmp/Metadata_Gene_Expression.tsv /Applications/tmp/Metadata_Gene_Expression.mp
