#!/bin/bash

function downloadFile {
  url="$1"
  fileName="tmp/$2"

  if [ ! -f "$fileName" ];
  then
    curl -o "$fileName" -L "$url"
  fi
}

mkdir -p tmp

#downloadFile "https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE92742&format=file&file=GSE92742%5FBroad%5FLINCS%5Finst%5Finfo%2Etxt%2Egz" "GSE92742_Broad_LINCS_inst_info.txt.gz" 
#downloadFile "https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE92742&format=file&file=GSE92742%5FBroad%5FLINCS%5Fgene%5Finfo%2Etxt%2Egz" "GSE92742_Broad_LINCS_gene_info.txt.gz"
#downloadFile "https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE92742&format=file&file=GSE92742%5FBroad%5FLINCS%5Fcell%5Finfo%2Etxt%2Egz" "GSE92742_Broad_LINCS_cell_info.txt.gz"
#downloadFile "https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE92742&format=file&file=GSE92742%5FBroad%5FLINCS%5Fpert%5Finfo%2Etxt%2Egz" "GSE92742_Broad_LINCS_pert_info.txt.gz"
#downloadFile "https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE92742&format=file&file=GSE92742%5FBroad%5FLINCS%5Fpert%5Fmetrics%2Etxt%2Egz" "GSE92742_Broad_LINCS_pert_metrics.txt.gz"
#downloadFile "https://www.ncbi.nlm.nih.gov/geo/download/?acc=GSE92742&format=file&file=GSE92742%5FBroad%5FLINCS%5FLevel3%5FINF%5Fmlr12k%5Fn1319138x12328%2Egctx%2Egz" "LINCS_PhaseI_Level3.gctx.gz"

cd tmp
gunzip "LINCS_PhaseI_Level3.gctx.gz"
