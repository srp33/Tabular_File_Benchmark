import os
import msgpack
import random
import subprocess
import argparse

from RandomAccessHelper import *

######################################################
# Args:
#   [1] Path to directory containing MessagePack files
#   [2] Path to input file
#   [3] Path to output file
######################################################

#############
# CONSTANTS
#############

POS_IX = 0
LENGTH_IX = 1


#############
# FUNCTIONS
#############


def random_tsv(input_path, db_path, output_path, gz_in, gz_out, num_samp):
    """Random TSV

    Randomly access tsv file to test RandomAccess functionality

    Args:
        input_path (str): Path to input file
        db_path (str): Path to LevelDB w/ sample ids and positions within gzipped file
        output_path (str): Path to output tsv
        gz_in (bool): Determines if input file is gzipped
        gz_out (bool): Determines if output file should be gzipped
        num_samp (int): Number of samples to pull from input file

    If there are less than num_samp samples in the given file, function will return without performing further actions

    """

    ########################################
    smart_print('Reading MessagePack files')
    ########################################

    if not os.path.exists(db_path):
        smart_print("\033[91mERROR:\033[0m Please create {} before running tests".format(db_path))
        return

    # with open('/'.join([db_path, 'sample_start.msgpack']), 'rb') as in_f:
    #     pos_map = msgpack.unpack(in_f)

    # with open('/'.join([db_path, 'sample_data_length.msgpack']), 'rb') as in_f:
    #     length_map = msgpack.unpack(in_f)

    with open('/'.join([db_path, 'sample_data.msgpack']), 'rb') as in_f:
        sample_data = msgpack.unpack(in_f)

    with open('/'.join([db_path, 'samples.msgpack']), 'rb') as in_f:
        samples = msgpack.unpack(in_f)

    nrows = len(samples)

    if nrows <= num_samp:
        smart_print("\033[93mWARNING:\033[0m Too few samples. Quitting program")
        return

    indices = []

    ##########################################
    smart_print('Generating random positions')
    ##########################################

    for i in range(num_samp):
        indices.append(random.randint(0, nrows))

    indices = sorted(indices)

    samples = [samples[i] for i in indices]
    ix_path = '/'.join([db_path, 'indices.gzidx'])

    ############################
    smart_print('Grabbing data')
    ############################

    with open(output_path, 'w') as out_file:
        output = []

        with dynamic_open(input_path, is_gzipped=gz_in, mode='r', index_file=ix_path) as in_file:
            # I'm sorry, I see no way to keep from repeating this, unless you know of some way to dynamically
            #   decode bytes to strings
            if gz_in:
                output = in_file.readline().decode()

                for sample in samples:
                    #smart_print("Reading data for {}".format(sample))
                    in_file.seek(sample_data[sample][POS_IX])
                    output += '\t'.join([sample.decode(), in_file.read(sample_data[sample][LENGTH_IX]).decode()]) + '\n'
            else:
                ####output = in_file.readline()
                out_file.write(in_file.readline())

                for sample in samples:
                    in_file.seek(sample_data[sample][POS_IX])
                    ####output += '\t'.join([sample.decode(), in_file.read(sample_data[sample][LENGTH_IX])]) + '\n'
                    output.append([sample.decode()] + in_file.read(sample_data[sample][LENGTH_IX]).split("\t")[:10])

        #############################
        smart_print('Writing output')
        #############################

        for items in output:
            out_file.write("\t".join(items) + "\n")

        if gz_out:
            subprocess.check_call(['gzip', output_path])
        smart_print('Done :)')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Get random amounts of data from specified file with accompanied "
                                                 "MessagePacks")
    parser.add_argument("msgpack_path", type=str, help="Path to MessagePack directory")
    parser.add_argument("in_path", type=str, help="Path to input file connected to MessagePack maps")
    parser.add_argument("out_path", type=str, help="Path for output file")
    parser.add_argument("-n", "--num_samples", type=int, default=1000,
                        help="Number of random samples to pull. Default: 1000")
    args = parser.parse_args()
    msgpack_path = args.msgpack_path
    in_path = args.in_path
    out_path = args.out_path
    num_samples = args.num_samples
    gzip_output = False

    if out_path.endswith('.gz'):
        gzip_output = True
        if os.path.exists(out_path):
            os.remove(out_path)
        out_path = out_path.replace('.gz', '')

    if in_path.endswith('.tsv.gz'):
        smart_print('File is gzipped. Starting up')
        random_tsv(in_path, msgpack_path, out_path, gz_in=True, gz_out=gzip_output, num_samp=num_samples)
    elif in_path.endswith('.tsv'):
        smart_print('Starting up')
        random_tsv(in_path, msgpack_path, out_path, gz_in=False, gz_out=gzip_output, num_samp=num_samples)
    else:
        smart_print("\033[91mInvalid input!\033[0m Please only use tsv or gzipped tsv files!")
