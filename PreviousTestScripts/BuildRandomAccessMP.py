from RandomAccessHelper import *
import os
import argparse


def map_tsv(in_file_path, output_dir, is_gzipped):
    """Map TSV using dictionary to be saved to MessagePack to hold position in file and length of data for sample

    Args:
          in_file_path (str): file path for input
          output_dir (str): path for database to be created
          is_gzipped (bool): is the input file gzipped?
    """
    samples = []
    example_samples = []

    if not os.path.exists(output_dir):
        os.mkdir(output_dir)

    sample_data = {}

    #################
    # ITERATE THROUGH
    #     FILE
    #################

    with dynamic_open(in_file_path, is_gzipped, 'r') as in_file:
        if is_gzipped:
            in_file.build_full_index()
            in_file.export_index('/'.join([output_dir, 'indices.gzidx']))

            feature_names = in_file.readline().decode().rstrip().split('\t')
        else:
            feature_names = in_file.readline().rstrip().split('\t')

        sample_ix = feature_names.index("Sample")

        # Get positioning after header (tell() cannot be called when using an iterator)
        cur_pos = in_file.tell()
        for i, line in enumerate(in_file):
            if i > 0 and i % 50000 == 0:
                smart_print(i)

            cur_len = len(line.encode('utf-8'))

            if is_gzipped:
                line = line.decode()

            data_list = line.rstrip().split('\t')
            sample = data_list[sample_ix]
            samples.append(sample)

            # Adding '\t' at the beginning ensures we get the correct string, however, we must add one to the index
            #   in order to get the correct positioning and not include the '\t'
            data_starter = '\t' + data_list[sample_ix + 1]
            cur_line_ix = line.index(data_starter) + 1
            start_ix = cur_line_ix + cur_pos

            data_length = len(line.rstrip('\n')) - cur_line_ix

            sample_data[sample] = (start_ix, data_length)

            cur_pos += cur_len

    in_file = dynamic_open(in_file_path, is_gzipped, 'r')
    in_file.close()

    open_msgpack('/'.join([output_dir, 'sample_data.msgpack']), 'wb', sample_data)

    open_msgpack('/'.join([output_dir, 'samples.msgpack']), 'wb', samples)

    open_msgpack('/'.join([output_dir, 'features.msgpack']), 'wb', feature_names)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create MessagePack files that map sample ids to their data")
    parser.add_argument("in_path", type=str, help="Path to input file")
    parser.add_argument("output_path", type=str, help="Path for output directory (will be created if does not exist)")

    args = parser.parse_args()
    in_path = args.in_path
    output_path = args.output_path

    if in_path.endswith('.tsv.gz'):
        smart_print('gzipped!')
        map_tsv(in_path, output_path, is_gzipped=True)
    elif in_path.endswith('.tsv'):
        smart_print('normal tsv')
        map_tsv(in_path, output_path, is_gzipped=False)
    else:
        smart_print("\033[91mInvalid input!\033[0m Please only use tsv or gzipped tsv files!")
