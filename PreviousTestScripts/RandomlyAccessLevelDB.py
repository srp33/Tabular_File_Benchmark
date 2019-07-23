import sys
import datetime
import plyvel
import random
import indexed_gzip as igz


def smart_print(x):
    """Print with current date and time

    Args:
        x: output to be printed
    """
    print("{} - {}".format(datetime.datetime.now(), x))
    sys.stdout.flush()


def random_gzipped(input_path, db_path, output_path):
    """Random Gzipped

    Randomly access gzipped file to test RandomAccess functionality

    Args:
        input_path (str): Path to gzipped input file
        db_path (str): Path to LevelDB w/ sample ids and positions within gzipped file
        output_path (str): Path to output tsv (non-gzipped)

    If there are 1000 or less samples in the LevelDB, function will return without performing further actions

    """
    try:
        level_map = plyvel.DB(db_path)
    except plyvel.Error:
        smart_print("Please create {} before running tests".format(db_path))
        return

    nrows = int(level_map.get(b'nrow').decode())
    pos_map = level_map.prefixed_db(b'position-')
    length_map = level_map.prefixed_db(b'length-')

    if nrows <= 1000:
        level_map.close()
        smart_print("Too few samples")
        return

    indices = []
    samples = []
    for i in range(1000):
        indices.append(random.randint(0, nrows))

    indices = sorted(indices)

    for i, key_val in enumerate(pos_map.iterator()):
        if i in indices:
            samples.append(key_val[0])

    with igz.IndexedGzipFile(input_path, mode='r') as in_file:
        output = in_file.readline().decode()
        for sample in samples:
            in_file.seek(int(pos_map.get(sample).decode()))
            output += '\t'.join([sample.decode(), in_file.read(int(length_map.get(sample).decode())).decode()]) + '\n'

    out_file = open(output_path, 'w')
    out_file.write(output)
    out_file.close()
    level_map.close()


def random_tsv(input_path, db_path, output_path):
    """Random Gzipped

    Randomly access tsv file to test RandomAccess functionality

    Args:
        input_path (str): Path to input file
        db_path (str): Path to LevelDB w/ sample ids and positions within gzipped file
        output_path (str): Path to output tsv

    If there are 1000 or less samples in the LevelDB, function will return without performing further actions

    """

    ###############################
    smart_print('Opening Database')
    ###############################

    try:
        level_map = plyvel.DB(db_path)
    except plyvel.Error:
        smart_print("\033[91mERROR:\033[0m Please create {} before running tests".format(db_path))
        return
    nrows = int(level_map.get(b'nrow').decode())
    pos_map = level_map.prefixed_db(b'position-')
    length_map = level_map.prefixed_db(b'length-')

    if nrows <= 1000:
        level_map.close()
        smart_print("\033[93mWARNING:\033[0m Too few samples. Quitting program")
        return

    indices = []
    samples = []

    ##########################################
    smart_print('Generating random positions')
    ##########################################

    for i in range(1000):
        indices.append(random.randint(0, nrows))

    indices = sorted(indices)

    ###################################
    smart_print('Gathering sample ids')
    ###################################

    for i, key_val in enumerate(pos_map.iterator()):
        if i in indices:
            samples.append(key_val[0])

    ############################
    smart_print('Grabbing data')
    ############################

    with open(input_path, mode='r') as in_file:
        output = in_file.readline()
        for sample in samples:
            in_file.seek(int(pos_map.get(sample).decode()))
            output += '\t'.join([sample.decode(), in_file.read(int(length_map.get(sample).decode()))]) + '\n'

    #############################
    smart_print('Writing output')
    #############################

    out_file = open(output_path, 'w')
    out_file.write(output)
    out_file.close()
    level_map.close()
    smart_print('Done :)')


if __name__ == "__main__":
    database_path = sys.argv[1]
    in_path = sys.argv[2]
    out_path = sys.argv[3]

    if in_path.endswith('.tsv.gz'):
        smart_print('File is gzipped. Starting up')
        random_gzipped(in_path, database_path, out_path)
    elif in_path.endswith('.tsv'):
        smart_print('Starting up')
        random_tsv(in_path, database_path, out_path)
    else:
        smart_print("\033[91mInvalid input!\033[0m Please only use tsv or gzipped tsv files!")
