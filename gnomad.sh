#wget https://storage.googleapis.com/gnomad-public/release/2.1.1/liftover_grch38/vcf/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.vcf.bgz
#wget https://storage.googleapis.com/gnomad-public/release/2.1.1/liftover_grch38/vcf/genomes/gnomad.genomes.r2.1.1.sites.liftover_grch38.vcf.bgz.tbi
#wget https://storage.googleapis.com/gnomad-public/release/3.0/vcf/genomes/gnomad.genomes.r3.0.sites.vcf.bgz
#wget https://storage.googleapis.com/gnomad-public/release/3.0/vcf/genomes/gnomad.genomes.r3.0.sites.vcf.bgz.tbi

#python3 ParseGnomad.py gnomad.genomes.r2.1.1.sites.liftover_grch38.vcf.bgz TestData/gnomad2.tsv.gz &
#python3 ParseGnomad.py gnomad.genomes.r3.0.sites.vcf.bgz TestData/gnomad3.tsv.gz &
#wait

# Remove downloaded files after parsing
rm -f gnomad.genomes.r2.1.1.sites.liftover_grch38.vcf.bgz TestData/gnomad2.tsv.gz
rm -f gnomad.genomes.r3.0.sites.vcf.bgz TestData/gnomad3.tsv.gz

python3 ConvertTsvToFixedWidthFile2.py TestData/gnomad2.tsv.gz TestData/gnomad2.fwf2 &
python3 ConvertTsvToFixedWidthFile2.py TestData/gnomad3.tsv.gz TestData/gnomad3.fwf2 &
wait
