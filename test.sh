#!/bin/bash

set -o errexit

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

#rm -rfv TestData
#mkdir -pv TestData

#time buildTestFiles 10 90 1000
#time buildTestFiles 100 900 1000000
#time buildTestFiles 100000 900000 1000

function runQuery {
  resultFile=$1
  numContinuous=$2
  numDiscrete=$3
  numRows=$4
  scriptFile=$5
  dataFileExtension=$6
  memMap=$7

  scriptName=$(basename $scriptFile)
  scriptName=${scriptName/\.py/}

  echo Testing $scriptFile
  dataFile=TestData/${numContinuous}_${numDiscrete}_${numRows}.$dataFileExtension
  outFile=/tmp/${scriptName}_${numContinuous}_${numDiscrete}_${numRows}_${dataFileExtension}_${memMap}.$dataFileExtension.out

  echo -e "$scriptFile\t$numContinuous\t$numDiscrete\t$numRows\t$memMap\t$( { /usr/bin/time -f %e python3 $scriptFile $dataFile $outFile $memMap > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 $scriptFile $dataFile $outFile $memMap

  masterOutFile=/tmp/TestSplit_${numContinuous}_${numDiscrete}_${numRows}_tsv_False.tsv.out

  # This compares against the output using the "ParseSplit" method
  if [[ "$scriptFile" != "TestSplit.py" ]]
  then
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

#runQueries Query_Results.tsv 10 90 1000
#runQueries Query_Results.tsv 100 900 1000000
#runQueries Query_Results.tsv 100000 900000 1000

function buildTestFiles2 {
  numContinuous=$1
  numDiscrete=$2
  numRows=$3

  python3 ConvertTsvToFixedWidthFile2.py TestData/${numContinuous}_${numDiscrete}_${numRows}.tsv TestData/${numContinuous}_${numDiscrete}_${numRows}.fwf2
}

#buildTestFiles2 10 90 1000
#buildTestFiles2 100 900 1000000
#buildTestFiles2 100000 900000 1000

function runQueries2 {
  resultFile=$1
  numContinuous=$2
  numDiscrete=$3
  numRows=$4

  ###runQuery $resultFile $numContinuous $numDiscrete $numRows TestSplit.py tsv True
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestFixedWidth2.py fwf2 True
}

#rm -f Query_Results_fwf2.tsv
#echo -e "Description\tNumContinuous\tNumDiscrete\tNumRows\tMemMap\tSeconds" > Query_Results_fwf2.tsv

#runQueries2 Query_Results_fwf2.tsv 10 90 1000
#runQueries2 Query_Results_fwf2.tsv 100 900 1000000
#runQueries2 Query_Results_fwf2.tsv 100000 900000 1000

function runQueries3 {
  resultFile=$1
  numContinuous=$2
  numDiscrete=$3
  numRows=$4

  ###runQuery $resultFile $numContinuous $numDiscrete $numRows TestSplit.py tsv True
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestFixedWidth3.py fwf2 True
}

##TODO: Incorporate this into runQueries3
#python3 TestFixedWidth3.py TestData/100_900_1000000.fwf2 /tmp/1 100 1000
#python3 TestFixedWidth3.py TestData/100000_900000_1000.fwf2 /tmp/1 100000 1000000
#runQueries3 Query_Results_fwf2.tsv 10 90 1000
#runQueries3 Query_Results_fwf2.tsv 100 900 1000000
#runQueries3 Query_Results_fwf2.tsv 100000 900000 1000




#rm -f Compression_Times.tsv
#echo -e "File\tMethod\tSeconds" > Compression_Times.tsv

function compressFile {
  f=$1
  method=$2
  level=$3

  #echo -e "$f\t$method\t$level\t$( { /usr/bin/time -f %e python3 CompressLines.py $f $method $level > /dev/null; } 2>&1 )" >> Compression_Times.tsv
  python3 CompressLines.py $f $method $level
}

#for f in TestData/*.fwf2
#for f in TestData/10_*.fwf2
#do
#  compressFile $f bz2 1
#  compressFile $f bz2 9
#  compressFile $f gz 1
#  compressFile $f gz 9
#  compressFile $f lzma NA
#  compressFile $f snappy NA
#done

function buildTestFiles3 {
  dim=$1

  python3 BuildTsvFileGenotypes.py $dim TestData/$dim.tmp
  #python3 ConvertTsvToFixedWidthFile.py TestData/${numContinuous}_${numDiscrete}_${numRows}.tsv TestData/${numContinuous}_${numDiscrete}_${numRows}.fwf
}

#buildTestFiles3 10
#buildTestFiles3 100
#buildTestFiles3 1000
#buildTestFiles3 10000
#buildTestFiles3 100000
#buildTestFiles3 1000000

#TODO for paper:
#  Filter on 1 discrete column and 1 numeric column for the 3 initial file sizes
#  Compression: snappy, gzip, bz2, lzma
#    Use the same filtering process (create TestFixedWidth4.py)
#    In CompressLines.py, save rowdict and coldict
#  Test on even larger file(s):
#    Variant as row, sample as column initially
#    Genotype in each cell
#    Pick 50 random rows and 50 random columns from:
#      10x10
#      100*100
#      1000*1000
#      ...
#      1000000*1000000 
#    Filter to 10 randomly selected rows and columns
#    Transpose entire file without reading more than a line into memory (see Archive/TestFixedWidthTranspose.py)
#      Monitor memory usage: https://stackoverflow.com/questions/774556/peak-memory-usage-of-a-linux-unix-process
#  Save file sizes (in a TSV file):
#    For each initial file format
#    After each type of compression

#TODO for Geney and WishBuilder:
#    Exclude sample and feature names?
#    Support filtering and building pandas dataframe
#    Need/want transposed files?
#    Store dictionaries in sqlitedict?
#      Test whether it's faster to store as msgpack or in key/value database
