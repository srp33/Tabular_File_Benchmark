#!/bin/bash

set -o errexit

#rm -rfv TestData
#mkdir -pv TestData

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

  python3 ConvertTsvToFixedWidthFile.py TestData/${numContinuous}_${numDiscrete}_${numRows}.tsv TestData/${numContinuous}_${numDiscrete}_${numRows}.fwf
}

#time buildTestFiles 90 11 1000
#time buildTestFiles 9000 1000 100000
#time buildTestFiles 90000 10000 10000

function runQuery {
  resultFile=$1
  numContinuous=$2
  numDiscrete=$3
  numRows=$4
  scriptFile=$5
  dataFileExtension=$6
  memMap=$7

  echo Testing $scriptFile
  dataFile=TestData/${numContinuous}_${numDiscrete}_${numRows}.$dataFileExtension
  outFile=/tmp/${numContinuous}_${numDiscrete}_${numRows}_${dataFileExtension}_${memMap}.$dataFileExtension.out

  echo -e "$scriptFile\t$numContinuous\t$numDiscrete\t$numRows\t$memMap\t$( { /usr/bin/time -f %e python3 $scriptFile $dataFile $outFile $memMap > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 $scriptFile $dataFile $outFile $memMap

  masterOutFile=/tmp/master.out

  # This compares against the output using the "ParseSplit" method
  if [[ "$scriptFile" == "TestSplit.py" ]]
  then
    cp $outFile $masterOutFile
  else
    python3 CheckOutput.py $outFile $masterOutFile
  fi
}

function runQueries {
  resultFile=$1
  numContinuous=$2
  numDiscrete=$3
  numRows=$4

  runQuery $resultFile $numContinuous $numDiscrete $numRows TestSplit.py tsv False
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestSplit.py tsv True
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestRegExQuantifiers.py tsv False
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestRegExQuantifiers.py tsv True
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestRegExTabs.py tsv False
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestRegExTabs.py tsv True
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestMsgPack.py msgpack False
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestMsgPack.py msgpack True
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestFlags.py flag False
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestFlags.py flag True
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestAwk.py tsv False
##  On wide file, mawk gave this type of error so I excluded it: "$32801 exceeds maximum field(32767)"
##  runQuery $resultFile $numContinuous $numDiscrete $numRows TestMawk.py tsv False
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestGawk.py tsv False
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestNawk.py tsv False
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestFixedWidth.py fwf False
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestFixedWidth.py fwf True
}

#rm -f Query_Results.tsv
#echo -e "Description\tNumContinuous\tNumDiscrete\tNumRows\tMemMap\tSeconds" > Query_Results.tsv

#runQueries Query_Results.tsv 90 11 1000
#runQueries Query_Results.tsv 9000 1000 100000
#runQueries Query_Results.tsv 90000 10000 10000

#rm -f Query_Results_fwf.tsv
#echo -e "Description\tNumContinuous\tNumDiscrete\tNumRows\tMemMap\tSeconds" > Query_Results_fwf.tsv

numContinuous=90
numDiscrete=11
numRows=1000
python3 ConvertTsvToFixedWidthFile2.py TestData/${numContinuous}_${numDiscrete}_${numRows}.tsv TestData/${numContinuous}_${numDiscrete}_${numRows}.fwf2

#TODO for Geney:
#  Optimize fixed width more
#    Remove extra space between columns
#    Save column location dict when building the file
#      Test whether it's faster to store as msgpack or in key/value database
#    Build file line map - scan to position of each value rather than reading full line
#    Test speed of transpose (informally)
#  Incorporate into WishBuilder
#    Exclude sample and feature names
#    Transpose
#    Store dictionaries in sqlitedict?

#TODO for paper:
#  File sizes (store in TSV file)
#  Compression: snappy, gzip, bz2, lzma

#for f in TestData/*.fixed
#do
#  echo "Compressing $f"
#  python3 CompressLinesSnappy.py $f $f.snappy
#done

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
##runQuery $numContinuous $numDiscrete $numRows TestMileposts.py mileposts # This is extremely slow
##runQuery $numContinuous $numDiscrete $numRows TestMileposts2.py mileposts # This is moderately slow
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
