# Add import statements
import argparse
from RandomAccessHelper import *

#################################################################
# I need to create a script that will merge two files that came
#   from the same original file. This will require the following:
#       [1] Metadata file
#       [2] Data file
#       [3, 4] MessagePack for the above files (I think we want
#               to use the [possibly] prefixed feature names)
#################################################################


def merge(file_paths, mp_paths, prefs, out_path, chunk):
    """Put those files together

    :param file_paths: List of file paths to be merged
    :param mp_paths: List of paths to MessagePack directories in same order as file_paths
    :param prefs: List of prefixes for features, same length and order as file_paths
    :param out_path: String path to output file
    :param chunk: Int indicating rows per chunk
    :return:
    """
    cur_files = {in_f: dynamic_open(in_f, mode='r', use_gzip_module=in_f.endswith(".gz")) for in_f in file_paths}
    mp_samples = {mp_p: open_msgpack('/'.join([mp_p, "samples.msgpack"]), mode='rb') for mp_p in mp_paths}
    mp_features = {mp_p: open_msgpack('/'.join([mp_p, "features.msgpack"]), mode='rb') for mp_p in mp_paths}
    mp_maps = {mp_p: open_msgpack('/'.join([mp_p, "sample_data.msgpack"]), mode='rb') for mp_p in mp_paths}

    all_features = []

    all_lines = []

    if prefs is not None:
        for ix, mp_path in enumerate(mp_paths):

            mp_features[mp_path] = ['__'.join([prefs[ix], x.decode()]) for x in mp_features[mp_path]]
            all_features.extend(mp_features[mp_path])

        output = '\t'.join(["Sample", *[feat for feat in all_features if not feat.endswith('Sample')]]) + '\n'
    else:
        for mp_path in mp_paths:
            all_features.extend(mp_features[mp_path])
        output = '\t'.join(["Sample", *[feat.decode() for feat in all_features if feat != b'Sample']]) + '\n'

    out_file = open(out_path, mode='w')

    # I apologize for how convoluted this is, but it just grabs all unique samples
    # all_samples = sorted(list(dict.fromkeys([x for k, v in mp_samples.items() for x in v])))
    all_samples = sorted(list(set().union(*[v for k, v in mp_samples.items()])))

    for mp_p in mp_paths:
        mp_samples[mp_p] = set(mp_samples[mp_p])
    for ix, sample in enumerate(all_samples):
        output += sample.decode() + '\t'
        for i, mp_dir in enumerate(mp_paths):
            if sample in mp_samples[mp_dir]:
                cur_file = cur_files[file_paths[i]]
                start_ix = mp_maps[mp_dir][sample][0]
                cur_file.seek(start_ix)
                cur_line = cur_file.read(mp_maps[mp_dir][sample][1]) + '\t'
                output = ''.join([output, cur_line])
            else:
                output += '\t'.join(["NA"] * (len(mp_features[mp_dir]) - 1)) + '\t'
        output = output.rstrip('\t')
        output += '\n'
        all_lines.append(output)
        output = ''
        if ix > 0 and ix % chunk == 0:
            smart_print(ix)
            out_file.write(''.join(all_lines))
            all_lines = []

    out_file.write(''.join(all_lines))
    #out_file.write(output)
    out_file.close()

    for cur_file in cur_files:
        cur_files[cur_file].close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Merge many files w/ same origin file")
    parser.add_argument("-f", "--files", nargs='*', type=str, help="List of files")
    parser.add_argument("-m", "--mp_dirs", nargs='*', type=str, help="List of MessagePack directories (Must be in "
                                                                     "same order as above files)")
    parser.add_argument('-p', '--prefixes', nargs='*', type=str, help="List of prefixes to use on features")
    parser.add_argument('-c', '--chunk_size', type=int, help="Rows to save in each chunk", default=50000)
    parser.add_argument('-o', '--output_file', type=str, help="Path to output file", default="merged_file.tsv")

    args = parser.parse_args()
    files = args.files
    mp_dirs = args.mp_dirs
    prefixes = args.prefixes
    chunk_size = args.chunk_size
    output_p = args.output_file

    merge(files, mp_dirs, prefixes, output_p, chunk_size)
