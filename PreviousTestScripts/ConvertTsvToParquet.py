import sys
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

inFilePath = sys.argv[1]
outFilePath = sys.argv[2]

#dfForSchema = pd.read_csv(inFilePath, sep='\t', nrows=10000)
dfForSchema = pd.read_csv(inFilePath, sep='\t', low_memory=False, na_values="NA")
print(dfForSchema.dtypes)
# We are having trouble where pandas is inferring types as object when there is mixed data, and
#  pyarrow doesn't like that (neither do I).
#parquet_schema = pa.Table.from_pandas(df=dfForSchema).schema
#print(parquet_schema)
sys.exit()
parquet_writer = pq.ParquetWriter(outFilePath, parquet_schema)

fileReader = pd.read_csv(inFilePath, sep='\t', chunksize=1000)

for i, chunk in enumerate(fileReader):
    print("Chunk {}".format(i))

#    if i == 0:
#        # Infer schema and open parquet file on first chunk
#        parquet_schema = pa.Table.from_pandas(df=chunk).schema
#        parquet_writer = pq.ParquetWriter(outFilePath, parquet_schema)

    table = pa.Table.from_pandas(chunk, schema=parquet_schema)
    parquet_writer.write_table(table)

parquet_writer.close()
