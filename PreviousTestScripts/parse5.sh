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

#python3 BuildDB5.py /Applications/tmp/test.tsv /Applications/tmp/test5_uncompressed.db /Applications/tmp/test5.db
#time python3 BuildDB5.py /Applications/tmp/Metadata.tsv /Applications/tmp/Metadata5_uncompressed.db /Applications/tmp/Metadata5.db
#time python3 BuildDB5.py /Applications/tmp/Gene_Expression.tsv /Applications/tmp/Gene_Expression5_uncompressed.db /Applications/tmp/Gene_Expression5.db

#python ParseExpression.py $gctxFileName $geneFile Gene_Expression.tsv.gz



#python3 ConvertTsvToParquet.py Metadata.tsv MetadataChunked.pq

#python3 ConvertTsvToParquet2.py Metadata.tsv Metadata.pq
#python3 ConvertTsvToParquet2.py /Applications/tmp/Gene_Expression.tsv /Applications/tmp/Gene_Expression.pq

#python3 QueryDB.py /Applications/tmp/Gene_Expression.db /tmp/1.tsv

#python3 BuildVedis5.py /Applications/tmp/test.tsv /Applications/tmp/test.vedis
#time python3 BuildVedis5.py /Applications/tmp/Metadata.tsv /Applications/tmp/Metadata.vedis
#time python3 BuildVedis5.py /Applications/tmp/Gene_Expression.tsv /Applications/tmp/Gene_Expression5.vedis
#python3 QueryVedis.py /Applications/tmp/Gene_Expression.vedis /tmp/2.tsv

#python3 BuildLevelDB.py /Applications/tmp/test.tsv /Applications/tmp/test.ldb
#time python3 BuildLevelDB.py /Applications/tmp/Metadata.tsv /Applications/tmp/Metadata.ldb
time python3 BuildLevelDB.py /Applications/tmp/Gene_Expression.tsv /Applications/tmp/Gene_Expression.ldb
