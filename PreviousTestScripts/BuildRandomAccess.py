import sys
import datetime
import plyvel
import os
import shutil
import indexed_gzip as igz


def smart_print(x):
    """Print with current date and time

    Args:
        x: output to be printed
    """
    print("{} - {}".format(datetime.datetime.now(), x))
    sys.stdout.flush()


def map_gzipped(in_file_path, database_file_path):
    """Map TSV using LevelDB to hold position in gzipped file and length of data for sample

    Args:
          in_file_path (str): gzipped file path for input
          database_file_path (str): path for database to be created
    """
    samples = []
    example_samples = []

    if os.path.exists(database_file_path):
        shutil.rmtree(database_file_path)

    level_map = plyvel.DB(database_file_path, create_if_missing=True)

    sample_start = level_map.prefixed_db(b'position-')
    sample_data_length = level_map.prefixed_db(b'length-')

    start_wb = sample_start.write_batch()
    length_wb = sample_data_length.write_batch()

    #################
    # ITERATE THROUGH
    #     FILE
    #################

    # print('Original text:')
    with igz.IndexedGzipFile(in_file_path, mode='r') as in_file:

        # Get features in file
        feature_names = in_file.readline().decode().rstrip().split('\t')
        sample_ix = feature_names.index("Sample")

        # Get positioning after header (tell() cannot be called when using an iterator)
        cur_pos = in_file.tell()
        for i, line in enumerate(in_file):
            if i > 0 and i % 50000 == 0:
                smart_print(i)
                start_wb.write()
                length_wb.write()

            cur_len = len(line)

            line = line.decode()

            data_list = line.rstrip().split('\t')
            sample = data_list[sample_ix]
            samples.append(sample)

            # Adding '\t' at the beginning ensures we get the correct string, however, we must add one to the index
            #   in order to get the correct positioning and not include the '\t'
            data_starter = '\t' + data_list[sample_ix + 1]
            cur_line_ix = line.index(data_starter) + 1
            start_ix = cur_line_ix + cur_pos

            start_wb.put(sample.encode(), str(start_ix).encode())

            data_length = len(line.rstrip('\n')) - cur_line_ix
            length_wb.put(sample.encode(), str(data_length).encode())

            # Print first sample, 41st sample, and 51st sample if they exist
            if i == 0 or i == 40 or i == 50:
                example_samples.append(sample)
            #            print(line.rstrip())
            cur_pos += cur_len
        start_wb.write()
        length_wb.write()
        level_map.put(b'nrow', str(i).encode())

    # print('\nMapped data:')

    in_file = igz.IndexedGzipFile(in_file_path, mode='r')

    # Print data for samples listed above, which should print identical to the above text
    # NOTE:
    #   What you see printed will be the sample id and the data for that sample joined by a tab, thus, the dictionary
    #       only stores the data with sample id as the key
    for sample in example_samples:
        in_file.seek(int(sample_start.get(sample.encode()).decode()))
        smart_print('\t'.join([sample, in_file.read(int(sample_data_length.get(sample.encode()).decode())).decode()]))

    in_file.close()
    level_map.close()


def map_tsv(in_file_path, database_file_path):
    """Map TSV using LevelDB to hold position in file and length of data for sample

    Args:
          in_file_path (str): file path for input
          database_file_path (str): path for database to be created
    """
    samples = []
    example_samples = []

    if os.path.exists(database_file_path):
        shutil.rmtree(database_file_path)

    level_map = plyvel.DB(database_file_path, create_if_missing=True)

    sample_start = level_map.prefixed_db(b'position-')
    sample_data_length = level_map.prefixed_db(b'length-')

    start_wb = sample_start.write_batch()
    length_wb = sample_data_length.write_batch()

    #################
    # ITERATE THROUGH
    #     FILE
    #################

    # print('Original text:')

    with open(in_file_path, 'r') as in_file:

        # Get features in file
        feature_names = in_file.readline().rstrip().split('\t')
        sample_ix = feature_names.index("Sample")

        # Get positioning after header (tell() cannot be called when using an iterator)
        cur_pos = in_file.tell()
        for i, line in enumerate(in_file):
            if i > 0 and i % 50000 == 0:
                smart_print(i)
                start_wb.write()
                length_wb.write()

            data_list = line.rstrip().split('\t')
            sample = data_list[sample_ix]
            samples.append(sample)

            # Adding '\t' at the beginning ensures we get the correct string, however, we must add one to the index
            #   in order to get the correct positioning and not include the '\t'
            data_starter = '\t' + data_list[sample_ix + 1]
            cur_line_ix = line.index(data_starter) + 1
            start_ix = cur_line_ix + cur_pos

            start_wb.put(sample.encode(), str(start_ix).encode())

            data_length = len(line.rstrip('\n')) - cur_line_ix
            length_wb.put(sample.encode(), str(data_length).encode())

            # Print first sample, 41st sample, and 51st sample if they exist
            if i == 0 or i == 40 or i == 50:
                example_samples.append(sample)
    #            print(line.rstrip())
            cur_pos += len(line)
        start_wb.write()
        length_wb.write()
        level_map.put(b'nrow', str(i).encode())

    # print('\nMapped data:')

    in_file = open(in_file_path, 'r')

    # Print data for samples listed above, which should print identical to the above text
    # NOTE:
    #   What you see printed will be the sample id and the data for that sample joined by a tab, thus, the dictionary
    #       only stores the data with sample id as the key
    for sample in example_samples:
        in_file.seek(int(sample_start.get(sample.encode()).decode()))
        smart_print('\t'.join([sample, in_file.read(int(sample_data_length.get(sample.encode()).decode()))]))

    in_file.close()
    level_map.close()


if __name__ == "__main__":
    in_path = sys.argv[1]
    # TODO: make this more sophisticated so there is a default value
    database_path = sys.argv[2]

    if in_path.endswith('.tsv.gz'):
        smart_print('gzipped!')
        map_gzipped(in_path, database_path)
    elif in_path.endswith('.tsv'):
        map_tsv(in_path, database_path)
    else:
        smart_print("\033[91mInvalid input!\033[0m Please only use tsv or gzipped tsv files!")
