import os
import shutil
import subprocess
import argparse
from RandomAccessHelper import *


def increment_chunk_name(chunk_name, ix):
    """Recursive function that will increment chunk_name by one if possible

    Args:
        chunk_name (list): current chunk identifier as list
        ix (int): what position we are currently incrementing
    Returns:
        (str): Incremented chunk name
    """
    if chunk_name[ix] == 'Z':
        if ix > 0:
            chunk_name[ix] = 'A'
            return increment_chunk_name(chunk_name, ix - 1)
        else:
            smart_print('\033[93mWARNING:\033[0m Reached chunk limit. Please refactor code to accept' +
                        'more chunks.')
            sys.exit()
    else:
        chunk_name[ix] = chr(ord(chunk_name[ix]) + 1)
        return ''.join(chunk_name)


def write_dict(my_dict, out_dir, keys, chunk_name):
    """Append dictionary values to out_path

    Args:
        my_dict (dict): Dictionary containing values to be appended to out_path
        out_dir (str): Path for output file
        keys (list): Keys in specific order (just to ensure proper ordering)
        chunk_name (str): Which chunk is being written
    """
    out_path = '/'.join([out_dir, chunk_name])
    if len(my_dict[keys[0]]) == 0:
        return
    if os.path.exists(out_path):
        smart_print("\033[93mWOAH BUDDY\033[0m chunk number didn't increase")
    else:
        with open(out_path, 'w') as out_file:
            for key in keys:
                out_file.write('\t'.join(my_dict[key]) + '\n')


def consolidate_files(data_dir, out_path, features):
    """Consolidate the chunk files together

    Args:
        data_dir (str): Directory where chunks were stored
        out_path (str): Path to final file
        features (list): List of features to put there
    """
    smart_print("Consolidating chunks")
    for walk_tuple in os.walk(data_dir):
        file_names = walk_tuple[2]
        file_names = sorted(file_names)

        cur_pos = {file_name: 0 for file_name in file_names}

        out_f = open(out_path, 'w')
        for feature in features:
            cur_line = feature.decode() + '\t'
            for file_name in file_names:
                in_f = open('/'.join([data_dir, file_name]), 'r')
                in_f.seek(cur_pos[file_name])
                cur_line += in_f.readline().rstrip() + '\t'
                cur_pos[file_name] = in_f.tell()
                in_f.close()

            cur_line = cur_line.rstrip('\t') + '\n'
            out_f.write(cur_line)
        out_f.close()


def transpose_tsv(in_path, mpack_dir, out_path, data_dir,
                  gz_in, gz_out, n_data_points):
    """Transpose a normal TSV

    Args:
        in_path (str): Path to the input file
        mpack_dir (str): Path to MessagePack folder
        out_path (str): Path for the output file
        data_dir (str): Path for temporary directory (will be deleted at end of program)
        gz_in (bool): Is the input gzipped?
        gz_out (bool): Whether or not output should be gzipped
        n_data_points (int): Number of data points per chunk (determines chunk size)
    """
    samples = open_msgpack('/'.join([mpack_dir, 'samples.msgpack']), 'rb')
    features = open_msgpack('/'.join([mpack_dir, 'features.msgpack']), 'rb')

    nrows = len(samples) + 1
    ncols = len(features)

    chunk_size, n_chunks = calculate_chunks(nrows, ncols, n_data_points)

    smart_print(chunk_size)
    smart_print(nrows)

    rows_perc_chunk = (chunk_size / nrows) * 100
    cur_percentage = 0

    feature_dict = {}

    # If you want to allow more chunks, just add 'A's to the end (currently allows >11 million chunks)
    chunk_name = 'AAAAA'

    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)

    os.mkdir(data_dir)

    try:

        with dynamic_open(in_path, mode='r', use_gzip_module=gz_in) as in_file:
            # We don't need the first line
            in_file.readline()

            for feature in features:
                feature_dict[feature] = []

            for ix, line in enumerate(in_file):
                if gz_in:
                    line = line.decode().strip().split('\t')
                else:
                    line = line.strip().split('\t')

                for feat_ix, feature in enumerate(features):
                    feature_dict[feature].append(line[feat_ix])

                if ix > 0 and ix % chunk_size == 0:
                    write_dict(feature_dict, data_dir, features, chunk_name)
                    feature_dict = reset_dict(feature_dict)
                    chunk_name = increment_chunk_name(list(chunk_name), len(chunk_name) - 1)
                    cur_percentage += rows_perc_chunk

                    ###############################################################
                    smart_print("{}% rows done".format(math.trunc(cur_percentage)))
                    ###############################################################

        #############################
        smart_print("100% rows done")
        #############################

        write_dict(feature_dict, data_dir, features, chunk_name)
        consolidate_files(data_dir, out_path, features)

        if gz_out:
            subprocess.check_call(['gzip', out_path])
    finally:
        shutil.rmtree(data_dir)
        smart_print("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transpose a TSV file that has accompanying MessagePack folder")
    parser.add_argument("in_file_path", type=str, help="Path to input file to be transposed")
    parser.add_argument("mpack_path", type=str, help="Path to MessagePack folder")
    parser.add_argument("out_file_path", type=str, help="Path for output file")
    parser.add_argument("-t", "--temp_dir", type=str, default="Transpose_dir.temp",
                        help="Specify where to put temporary directory (will be deleted at end of program)" +
                        " Default: temp_dir (will be put in current directory)")
    parser.add_argument("-g", "--gzip", action="store_true", help="gzip output file")
    parser.add_argument("-n", "--num_data_points", type=int, default=500000000,
                        help="Number of data points per chunk (determines chunk size) Default: 500,000,000")
    args = parser.parse_args()
    in_file_path = args.in_file_path
    mpack_path = args.mpack_path
    out_file_path = args.out_file_path
    temp_dir = args.temp_dir
    compress_output = args.gzip
    num_data_points = args.num_data_points

    # Output file will only be gzipped if input file is gzipped
    #   We're aiming for consistency, my good man
    if out_file_path.endswith('.gz'):
        out_file_path = out_file_path.replace('.gz', '')
        # smart_print("Gzip usage is based on compression status of original file. '.gz' removed from output file path")

    if os.path.exists(out_file_path):
        os.remove(out_file_path)

    if in_file_path.endswith(".tsv.gz"):
        if os.path.exists('.'.join([out_file_path, 'gz'])):
            os.remove('.'.join([out_file_path, 'gz']))
        index_file = '/'.join([mpack_path, 'indices.gzidx'])
        transpose_tsv(in_file_path, mpack_path, out_file_path, temp_dir,
                      gz_in=True, gz_out=compress_output, n_data_points=num_data_points)
    elif in_file_path.endswith(".tsv"):
        transpose_tsv(in_file_path, mpack_path, out_file_path, temp_dir,
                      gz_in=False, gz_out=compress_output, n_data_points=num_data_points)
    else:
        smart_print("Invalid file type! Quitting program")
