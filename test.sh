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

#runQueries Query_Results.tsv 90 11 1000
#runQueries Query_Results.tsv 9000 1000 100000
#runQueries Query_Results.tsv 90000 10000 10000

function buildTestFiles2 {
  numContinuous=$1
  numDiscrete=$2
  numRows=$3

  python3 ConvertTsvToFixedWidthFile2.py TestData/${numContinuous}_${numDiscrete}_${numRows}.tsv TestData/${numContinuous}_${numDiscrete}_${numRows}.fwf2
}

#buildTestFiles2 90 11 1000
#buildTestFiles2 9000 1000 100000
#buildTestFiles2 90000 10000 10000

function runQueries2 {
  resultFile=$1
  numContinuous=$2
  numDiscrete=$3
  numRows=$4

  ####runQuery $resultFile $numContinuous $numDiscrete $numRows TestSplit.py tsv True
  runQuery $resultFile $numContinuous $numDiscrete $numRows TestFixedWidth2.py fwf2 True
}

#rm -f Query_Results_fwf.tsv
#echo -e "Description\tNumContinuous\tNumDiscrete\tNumRows\tMemMap\tSeconds" > Query_Results_fwf.tsv

#runQueries2 Query_Results_fwf.tsv 90 11 1000
#runQueries2 Query_Results_fwf.tsv 9000 1000 100000
#runQueries2 Query_Results_fwf.tsv 90000 10000 10000

function runTransposeQuery {
  resultFile=$1
  numContinuous=$2
  numDiscrete=$3
  numRows=$4
  scriptFile=$5
  dataFileExtension=$6

  scriptName=$(basename $scriptFile)
  scriptName=${scriptName/\.py/}

  echo Testing $scriptFile
  dataFile=TestData/${numContinuous}_${numDiscrete}_${numRows}.$dataFileExtension
  outFile=/tmp/${scriptName}_${numContinuous}_${numDiscrete}_${numRows}_${dataFileExtension}_Transpose.$dataFileExtension.out

  echo -e "$scriptFile\t$numContinuous\t$numDiscrete\t$numRows\t$( { /usr/bin/time -f %e python3 $scriptFile $dataFile $outFile > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 $scriptFile $dataFile $outFile

  masterOutFile=/tmp/TestSplitTranspose_${numContinuous}_${numDiscrete}_${numRows}_tsv_False.tsv.out

  # This compares against the output using the "ParseSplit" method
  if [[ "$scriptFile" != "TestSplitTranspose.py" ]]
  then
    python3 CheckOutput.py $outFile $masterOutFile
  fi
}

function runTransposeQueries {
  resultFile=$1
  numContinuous=$2
  numDiscrete=$3
  numRows=$4

  runTransposeQuery $resultFile $numContinuous $numDiscrete $numRows TestSplitTranspose.py tsv
  #runTransposeQuery $resultFile $numContinuous $numDiscrete $numRows TestFixedWidthTranspose.py fwf2
}

rm -f Query_Results_transpose.tsv
echo -e "Description\tNumContinuous\tNumDiscrete\tNumRows\tSeconds" > Query_Results_transpose.tsv

runTransposeQueries Query_Results_transpose.tsv 90 11 1000
runTransposeQueries Query_Results_transpose.tsv 9000 1000 100000
runTransposeQueries Query_Results_transpose.tsv 90000 10000 10000

#time python3 TransposeTSV.py TestData/9000_1000_100000.tsv TestData/9000_1000_100000.tsv.transposed
#time python3 TransposeTSV.py TestData/90000_10000_10000.tsv TestData/90000_10000_10000.tsv.transposed



#rm -f Compression_Times.tsv
#echo -e "File\tMethod\tSeconds" > Compression_Times.tsv

#for f in TestData/*.fwf2
#do
#  for method in bz2 gz lzma snappy
#  do
#    echo "Compressing $f with $method"
#    echo -e "$f\t$method\t$( { /usr/bin/time -f %e python3 CompressLines.py $f $method > /dev/null; } 2>&1 )" >> Compression_Times.tsv
#  done
#done

#TODO for paper:
#  Pick half rows at random
#  Pick half rows at random, then transpose
#  Test on even larger file(s):
#    1 character per cell
#    Pick 50 random rows and 50 random columns from:
#      10x10
#      100*100
#      1000*1000
#      ...
#      1000000*1000000 
#    Transpose each file without reading more than a line into memory
#  Compression: snappy, gzip, bz2, lzma
#    Test compresslevel=1 and compresslevel=5 for gzip, bz2
#    File sizes (store in TSV file)

#TODO for Geney and WishBuilder:
#    Exclude sample and feature names?
#    Support filtering and building pandas dataframe
#    Need transposed files?
#    Store dictionaries in sqlitedict?
#      Test whether it's faster to store as msgpack or in key/value database
