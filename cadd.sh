wget https://krishna.gs.washington.edu/download/CADD/v1.6/GRCh38/whole_genome_SNVs_inclAnno.tsv.gz
#wget https://krishna.gs.washington.edu/download/CADD/v1.6/GRCh38/whole_genome_SNVs_inclAnno.tsv.gz.tbi

zcat whole_genome_SNVs_inclAnno.tsv.gz | head -n 2 | tail -n +2 | cut -c2- | gzip > TestData/cadd.tsv.gz
zcat whole_genome_SNVs_inclAnno.tsv.gz | tail -n +3 | gzip >> TestData/cadd.tsv.gz

#Remove downloaded files after parsing?
#rm -f whole_genome_SNVs_inclAnno.tsv.gz whole_genome_SNVs_inclAnno.tsv.gz.tbi
