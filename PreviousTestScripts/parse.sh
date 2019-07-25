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
##python3 convertTallFormatToWide.py tmp/Metadata.tsv.gz Metadata.tsv.gz

#python3 BuildDB.py /Applications/tmp/test.tsv /Applications/tmp/test.db
#time python3 BuildDB.py /Applications/tmp/Metadata.tsv /Applications/tmp/Metadata.db


#python ParseExpression.py $gctxFileName $geneFile Gene_Expression.tsv.gz
