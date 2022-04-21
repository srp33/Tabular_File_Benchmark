#!/bin/bash

set -o errexit

d=$(pwd)

mkdir -p Results2 TestData
#rm -rf Results2/* TestData/*

tmpDir=/tmp/FWF2_$(date "+%Y%m%d-%H%M%S")
rm -rf $tmpDir
mkdir -p $tmpDir

cp Environment/Dockerfile $tmpDir/
cp Environment/build_docker $tmpDir/
cp test.sh $tmpDir/
cp *.py $tmpDir/
cp -r Rust $tmpDir/Rust/
cp *.R $tmpDir/
cp *.cpp $tmpDir/

mkdir -p $tmpDir/F4/f4py
cp F4/f4py/* $tmpDir/F4/f4py/

cd $tmpDir
./build_docker

#docker run -i -t --rm \
docker run -d --rm \
  -v $d/Results2:/Results2 \
  -v $d/TestData:/TestData \
  -v $tmpDir:/tmp \
  --user $(id -u):$(id -g) \
  srp33/tab_bench bash /test.sh

cd ..
rm -rf $tmpDir
