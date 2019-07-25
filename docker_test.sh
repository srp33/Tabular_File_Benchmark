#!/bin/bash

set -o errexit

d=$(pwd)

#mkdir -p Results2 TestData
#rm -rf Results2/* TestData/*

rm -rf tmp
mkdir -p tmp

cp Environment/Dockerfile tmp/
cp Environment/build_docker tmp/
cp test.sh tmp/
cp *.py tmp/
cp *.R tmp/
cp TestFixedWidth2.cpp tmp/

cd tmp

./build_docker

#docker run -i -t --rm \
docker run --rm \
  -v $d/Results2:/Results2 \
  -v $d/TestData:/TestData \
  -v /tmp:/tmp \
  --user $(id -u):$(id -g) \
  srp33/tab_bench ./test.sh
#  srp33/tab_bench /bin/bash

cd ..
rm -rf tmp
