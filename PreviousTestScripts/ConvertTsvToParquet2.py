import os, shutil, sys
import dask.dataframe as dd
import pandas as pd

inFilePath = sys.argv[1]
outFilePath = sys.argv[2]

engine = "pyarrow"

colPrefix = os.path.basename(inFilePath).replace(".tsv", "")

##df = dd.read_csv(inFilePath, sep="\t", blocksize=100e6, sample=4000000000, low_memory=False)
#df = dd.read_csv(inFilePath, sep="\t", dtype=str, blocksize=100e6, sample=4000000000)
#df = df.rename(columns={c: colPrefix + "_" + c for c in df.columns})

#if os.path.exists(outFilePath):
#    shutil.rmtree(outFilePath)
#df.to_parquet(outFilePath, engine=engine, compression="gzip")

df2 = dd.read_parquet(outFilePath, engine=engine, columns=['BRCA1', 'HPRT1'])
#df2 = dd.read_parquet(outFilePath, engine=engine, columns=['Metadata_cell_type', 'Metadata_donor_age'])
#df2 = dd.read_parquet(outFilePath, engine=engine)
#df2.compute().to_csv("/tmp/1.tsv", sep="\t")
# 2 minutes 5 seconds

#df2 = dd.read_parquet(outFilePath, engine=engine)

#df3 = pd.read_parquet(outFilePath, engine=engine)
# > 5 minutes and lots of memory (as expected)

#df3 = pd.read_parquet(outFilePath, engine=engine, columns=['BRCA1', 'HPRT1'])
# 2 minutes, 24 seconds
