import datetime
import sys
import gzip as gz
import math
import msgpack
import indexed_gzip as igz

#########################################
# This file is meant to house functions
#   that are used across multiple scripts
#########################################


def smart_print(x):
    """Print with current date and time

    Args:
        x: output to be printed
    """
    print("{} - {}".format(datetime.datetime.now(), x))
    sys.stdout.flush()


def reset_dict(my_dict):
    """Simple function to reset values of dictionary

    Args:
        my_dict (dict): Dictionary to be reset

    Dictionary must have keys already
    """

    for k in my_dict:
        my_dict[k] = []

    return my_dict


def calculate_chunks(nrows, ncols, n_data_points=500000000):
    """Dynamically calculate chunk size

    Args:
        nrows (int): Number of rows in tsv file
        ncols (int): Number of columns in tsv file
        n_data_points (int): About how many data points per chunk
    """

    chunk_size = math.floor(n_data_points / ncols)
    if chunk_size > nrows:
        chunk_size = math.floor(nrows / 2)
    n_chunks = nrows / chunk_size
    return chunk_size, n_chunks


def dynamic_open(file_path, is_gzipped=False, mode='r', index_file=None, use_gzip_module=False):
    """Dynamically open a file

    Args:
        file_path (str): Path to file
        is_gzipped (bool): Is the file gzipped
        mode (str): 'r', 'w', blah blah
        index_file (str): Pass this bad boi into the index file slot
        use_gzip_module (bool): Override indexed_gzip for when we don't need an index file
    """
    if use_gzip_module:
        return gz.open(filename=file_path, mode=mode)
    elif is_gzipped:
        return igz.IndexedGzipFile(filename=file_path, mode=mode, index_file=index_file)
    else:
        return open(file_path, mode)


def open_msgpack(file_path, mode, store_data=None):
    """Access msgpack

    Args:
        file_path (str): Path to msgpack file, either to be created or already existent
        mode (str): Either 'rb' or 'wb' (file must be read as bytes)
        store_data (dict or list): Object to store if mode == 'wb'

    Returns:
        Dictionary created from unpack or whatever you get from packing
    """
    with open(file_path, mode) as cur_file:
        if mode == 'rb':
            return msgpack.unpack(cur_file)
        elif mode == 'wb':
            if store_data is None:
                smart_print("\033[91mWARNING:\033[0m No data given to pack")
            return msgpack.pack(store_data, cur_file)
        else:
            raise ValueError("`mode` must be either 'rb' or 'wb'")
