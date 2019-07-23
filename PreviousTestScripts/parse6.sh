#!/bin/bash

#sudo apt-get install build-essential libsnappy-dev zlib1g-dev libbz2-dev libgflags-dev

##############################################################
# RocksDB bindings
##############################################################

git clone https://github.com/facebook/rocksdb.git
cd rocksdb
mkdir build && cd build
cmake ..
make

export CPLUS_INCLUDE_PATH=${CPLUS_INCLUDE_PATH}:`pwd`/../include
export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:`pwd`
export LIBRARY_PATH=${LIBRARY_PATH}:`pwd`

#sudo apt-get install python-virtualenv python-dev
virtualenv venv
source venv/bin/activate
pip install git+git://github.com/twmht/python-rocksdb.git#egg=python-rocksdb

#python3 BuildLevelDB.py /Applications/tmp/test.tsv /Applications/tmp/test.ldb
#time python3 BuildLevelDB.py /Applications/tmp/Metadata.tsv /Applications/tmp/Metadata.ldb
#time python3 BuildLevelDB.py /Applications/tmp/Gene_Expression.tsv /Applications/tmp/Gene_Expression.ldb
