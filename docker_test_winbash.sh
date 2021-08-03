#!/bin/bash

set -o errexit

d="$(pwd)"
tmpDir=Temp_$(date "+%Y%m%d-%H%M%S")

rm -rf Temp_*
mkdir -p "Results2" "TestData" "$tmpDir"
rm -rf "$d/Results2"/* "$d/TestData"/*

cp Environment/Dockerfile $tmpDir/
cp Environment/build_docker $tmpDir/
cp test.sh $tmpDir/
cp *.py $tmpDir/
cp *.rs $tmpDir/
cp *.R $tmpDir/
cp *.cpp $tmpDir/

cd $tmpDir
./build_docker

#docker run -i -t --rm \
docker run --rm \
  -v "/$d/Results2":/Results2 \
  -v "/$d/TestData":/TestData \
  -v "/$d/$tmpDir":/tmp \
  --user $(id -u):$(id -g) \
  srp33/tab_bench ./test.sh
#  srp33/tab_bench /bin/bash

cd ..
rm -rf $tmpDir
