#!/bin/bash

set -o errexit

rm -rf TestData
mkdir -p TestData

function buildTestFile {
  numContinuous=$1
  numDiscrete=$2
  numRows=$3
  scriptFile=$4
  dataFileExtension=$5

  dataFile=TestData/${numContinuous}_${numDiscrete}_${numRows}.$dataFileExtension

  python3 $scriptFile $numContinuous $numDiscrete $numRows $dataFile
}

function buildTestFiles {
  numContinuous=$1
  numDiscrete=$2
  numRows=$3

  buildTestFile $numContinuous $numDiscrete $numRows BuildTsvFile.py tsv &
  buildTestFile $numContinuous $numDiscrete $numRows BuildMsgPackFile.py msgpack &
  buildTestFile $numContinuous $numDiscrete $numRows BuildFlagFile.py flag &
  wait
}

buildTestFiles 90 11 1000
buildTestFiles 9000 1000 100000

function runQuery {
  numContinuous=$1
  numDiscrete=$2
  numRows=$3
  scriptFile=$4
  dataFileExtension=$5

  echo Testing $scriptFile
  dataFile=TestData/${numContinuous}_${numDiscrete}_${numRows}.$dataFileExtension
  outFile=/tmp/$scriptFile.out

  ####echo -e "$scriptFile\t$numContinuous\t$numDiscrete\t$numRows\t$( { /usr/bin/time -f %e python3 $scriptFile $dataFile $outFile > /dev/null; } 2>&1 )" >> Query_Results.tsv
  time python3 $scriptFile $dataFile $outFile

  if [[ "$scriptFile" != "TestParseSplit.py" ]]
  then
    masterOutFile=/tmp/TestParseSplit.py.out
    python3 CheckOutput.py $outFile $masterOutFile
  fi
}

rm -f Query_Results.tsv
echo -e "Script\tNumContinuous\tNumDiscrete\tNumRows\tSeconds" > Query_Results.tsv

#numContinuous=90
#numDiscrete=11
#numRows=1000
numContinuous=9000
numDiscrete=1000
numRows=100000

#runQuery $numContinuous $numDiscrete $numRows TestParseSplit.py tsv
#runQuery $numContinuous $numDiscrete $numRows TestParseRegEx.py tsv
#runQuery $numContinuous $numDiscrete $numRows TestParseRegEx2.py tsv
#runQuery $numContinuous $numDiscrete $numRows TestParseRegExMemMap.py tsv
#runQuery $numContinuous $numDiscrete $numRows TestParseMsgPack.py msgpack
##runQuery $numContinuous $numDiscrete $numRows TestMileposts.py mileposts # This is extremely slow
##runQuery $numContinuous $numDiscrete $numRows TestMileposts2.py mileposts # This is moderately slow
#runQuery $numContinuous $numDiscrete $numRows TestMileposts3.py mileposts # find_nths, reasonably fast
#runQuery $numContinuous $numDiscrete $numRows TestAwk.py tsv
#runQuery $numContinuous $numDiscrete $numRows TestMawk.py tsv
#runQuery $numContinuous $numDiscrete $numRows TestGawk.py tsv
#runQuery $numContinuous $numDiscrete $numRows TestNawk.py tsv
#runQuery $numContinuous $numDiscrete $numRows TestFixedWidth.py fixed
#runQuery $numContinuous $numDiscrete $numRows TestFixedWidthMemMap.py fixed


#for f in TestData/*.fixed
#do
#  echo "Compressing $f"
#  python3 CompressLinesSnappy.py $f $f.snappy
#done



#TODO:
#  Clean up
#  Apply mmap to all methods possible (and simplify some things?)
#  Compression
#  Run tests on a really wide file.
#  Store file sizes in TSV file
#  Fine tune for Geney

#BACKBURNER:
#  Write tests that pick 1000 noncontiguous rows from the file
#    Write a Python script that seeks to lines and pipes it to stdout
#    line by line generators: https://docs.python.org/3/library/itertools.html
#    awk?
#  Parallelize?
#  newlines approach + consume()?
#  sqlitedict?
#  LevelDB?
#  Vedis?
#  C++ string splitting: https://github.com/tobbez/string-splitting
#    http://book.pythontips.com/en/latest/python_c_extension.html
#    Cython

#NOTES:
##buildTestFile $numContinuous $numDiscrete $numRows BuildMsgPackFileSnappy.py msgpack.snappy
##buildTestFile $numContinuous $numDiscrete $numRows BuildMsgPackFileGzip.py msgpack.gz
##buildTestFile $numContinuous $numDiscrete $numRows BuildMsgPackFileBz2.py msgpack.bz2
##buildTestFile $numContinuous $numDiscrete $numRows BuildMsgPackFileLzma.py msgpack.lzma
#awkQuery='{out=""; for(i=0;i<10000;i+=100){out=out$i"\t"}; print out}'
#sedFilter='s/^[ \t]*//;s/[ \t]*$//'
#time awk -v OFS='\t' '{out=""; for(i=10;i<=1000;i+=10){out=out" "$i}; print out}' TestFile_10000_100_200000_tabs.tsv > /dev/null #31 seconds
#time mawk -v OFS='\t' '{out=""; for(i=10;i<=1000;i+=10){out=out" "$i}; print out}' TestFile_10000_100_200000_tabs.tsv > /dev/null #31 seconds
#time gawk -v OFS='\t' '{out=""; for(i=10;i<=1000;i+=10){out=out" "$i}; print out}' TestFile_10000_100_200000_tabs.tsv > /dev/null #13 seconds
#time nawk -v OFS='\t' "$awkQuery" TestData/9000_1000_100000.tsv | sed "$sedFilter" > /dev/null
##time awk ' {print $1, $9;}' TestFile_10000_100_200000_tabs.tsv | wc -l
##time awk 'NR>=150001&&NR<=151000 {print $1, $9;}' TestFile_10000_100_200000_tabs.tsv | wc -l
##time awk '(NR>=150001&&NR<=151000)||(NR>=160001&&NR<=161000) {print $1, $9;}' TestFile_10000_100_200000_tabs.tsv | wc -l
##time awk -v OFS='\t' '(NR>=150001&&NR<=151000)||(NR>=160001&&NR<=161000) {print $9500, $9800;}' TestFile_10000_100_200000_tabs.tsv | wc -l
##time awk -v OFS='\t' '(NR>=1&&NR<=151000)||(NR>=160001&&NR<=161000) {print $9500, $9800;}' TestFile_10000_100_200000_tabs.tsv | wc -l
##time awk -v OFS='\t' ' {print $9500, $9800;}' TestFile_10000_100_200000_tabs.tsv | wc -l
##time awk -v OFS='\t' ' {print $5, $15;}' TestFile_10000_100_200000_tabs.tsv | wc -l
##time mawk -v OFS='\t' ' {print $5, $15;}' TestFile_10000_100_200000_tabs.tsv | wc -l
##time gawk -v OFS='\t' ' {print $5, $15;}' TestFile_10000_100_200000_tabs.tsv | wc -l
##time nawk -v OFS='\t' ' {print $5, $15;}' TestFile_10000_100_200000_tabs.tsv | wc -l
##time python3 TestParsePickle.py TestFile_10000_100_200000_tabs.pkl # Can't get it to work because pickled objects have newline characters
