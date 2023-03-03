import gzip
import h5py
import numpy as np
import pandas as pd
import sys

in_file_path = sys.argv[1]
out_samples_file_path = sys.argv[2]
out_expr_file_path = sys.argv[3]

########################################################
# Parse sample data
########################################################

samples_h5 = h5py.File(in_file_path)["meta"]["samples"]

#print(list(samples_h5.keys()))
#def peek(key):
#    print(key, flush=True)
#    x = sorted(list(set([x for x in samples_h5[key]])))
#    print(x[:min(50, len(x))], flush=True)

#prefixes = set()
#for x in sample_dict["characteristics_ch1"]:
#    y = x.split(b"\t")
#    for a in y:
#        prefixes.add(a.split(b": ")[0])
#print(len(list(prefixes)))
# INFO: There are 6151 unique prefixes

sample_keys_to_keep = ["characteristics_ch1", "geo_accession", "instrument_model", "molecule_ch1", "readstotal", "series_id", "singlecellprobability", "source_name_ch1", "taxid_ch1", "title", "type"]

sample_dict = {}
for key in sample_keys_to_keep:
    print(f"Retrieving metadata for {key}", flush=True)
    if key in ["readstotal", "singlecellprobability"]:
        sample_dict[key] = [str(x).encode() for x in samples_h5[key]]
    else:
        sample_dict[key] = [x.replace(b"\t", b";") for x in samples_h5[key]]

with gzip.open(out_samples_file_path, "w") as out_file:
    out_file.write(b"\t".join([x.encode() for x in sample_keys_to_keep]) + b"\n")

    for i in range(len(sample_dict[sample_keys_to_keep[0]])):
        out_row = [sample_dict[key][i] for key in sample_keys_to_keep]
        out_file.write(b"\t".join(out_row) + b"\n")

########################################################
# Parse expression data
########################################################

transcripts_h5 = h5py.File(in_file_path)["meta"]["transcripts"]
ensembl_gene_ids = [x for x in transcripts_h5["ensembl_gene_id"]]
ensembl_transcript_ids = [x for x in transcripts_h5["ensembl_transcript_id"]]
gene_symbols = [x for x in transcripts_h5["gene_symbol"]]
gene_biotypes = [x for x in transcripts_h5["gene_biotype"]]

expr_h5 = h5py.File(in_file_path)["data"]["expression"]

with gzip.open(out_expr_file_path, "w") as out_file:
    out_file.write(b"\t".join([b"ensembl_gene_id", b"ensembl_transcript_id", b"gene_symbol", b"gene_biotype"] + sample_dict["geo_accession"]) + b"\n")

    chunk_size = 1000
    for i in range(0, expr_h5.shape[0], chunk_size):
        print(f"Retrieving expression data for row {i}", flush=True)

        end_i = min([i + chunk_size, expr_h5.shape[0]])
        chunk = np.array(expr_h5[i:end_i,:]).tolist()

        for row in chunk:
            out_items = [ensembl_gene_ids.pop(0), ensembl_transcript_ids.pop(0), gene_symbols.pop(0), gene_biotypes.pop(0)] + [str(x).encode() for x in row]
            out_file.write(b"\t".join(out_items) + b"\n")
