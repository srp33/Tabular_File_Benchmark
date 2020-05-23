wget https://storage.googleapis.com/gnomad-public/release/2.1.1/vcf/genomes/gnomad.genomes.r2.1.1.sites.vcf.bgz
wget https://storage.googleapis.com/gnomad-public/release/2.1.1/vcf/genomes/gnomad.genomes.r2.1.1.sites.vcf.bgz.tbi
#wget https://storage.googleapis.com/gnomad-public/release/3.0/vcf/genomes/gnomad.genomes.r3.0.sites.vcf.bgz
#wget https://storage.googleapis.com/gnomad-public/release/3.0/vcf/genomes/gnomad.genomes.r3.0.sites.vcf.bgz.tbi

python3 ParseGnomad.py gnomad.genomes.r2.1.1.sites.vcf.bgz TestData/gnomad2.tsv.gz &
python3 ParseGnomad.py gnomad.genomes.r3.0.sites.vcf.bgz TestData/gnomad3.tsv.gz &
wait

#TODO: Remove downloaded files after parsing?
