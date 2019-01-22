#!/bin/bash

set -o errexit

############################################################
# Prep and clean before beginning analysis.
############################################################

rm -rfv TestData
mkdir -pv TestData

############################################################
# Build first round of test files.
############################################################

function buildTestFile {
  numDiscrete=$1
  numContinuous=$2
  numRows=$3
  scriptFile=$4
  dataFileExtension=$5

  dataFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.$dataFileExtension

  python3 $scriptFile $numDiscrete $numContinuous $numRows $dataFile
}

function buildTestFiles {
  numDiscrete=$1
  numContinuous=$2
  numRows=$3

  buildTestFile $numDiscrete $numContinuous $numRows BuildTsvFile.py tsv &
  buildTestFile $numDiscrete $numContinuous $numRows BuildMsgPackFile.py msgpack &
  buildTestFile $numDiscrete $numContinuous $numRows BuildFlagFile.py flag &
  wait

  python3 ConvertTsvToFixedWidthFile.py TestData/${numDiscrete}_${numContinuous}_${numRows}.tsv TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf
}

## Small files
time buildTestFiles 10 90 1000
## Tall, narrow files
time buildTestFiles 100 900 1000000
## Short, wide files
time buildTestFiles 100000 900000 1000

############################################################
# Query every 100th column from first round of test files
# using a variety of methods.
############################################################

function runQuery {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4
  scriptFile=$5
  dataFileExtension=$6
  memMap=$7

  scriptName=$(basename $scriptFile)
  scriptName=${scriptName/\.py/}

  echo Testing $scriptFile
  dataFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.$dataFileExtension
  outFile=/tmp/${scriptName}_${numDiscrete}_${numContinuous}_${numRows}_${dataFileExtension}_${memMap}.$dataFileExtension.out

  echo -e "$scriptFile\t$numDiscrete\t$numContinuous\t$numRows\t$memMap\t$( { /usr/bin/time -f %e python3 $scriptFile $dataFile $outFile $memMap > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 $scriptFile $dataFile $outFile $memMap

  masterOutFile=/tmp/TestSplit_${numDiscrete}_${numContinuous}_${numRows}_tsv_False.tsv.out

  # This compares against the output using the "ParseSplit" method
  if [[ "$scriptFile" != "TestSplit.py" ]]
  then
    python3 CheckOutput.py $outFile $masterOutFile
  fi
}

function runQueries {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4

  runQuery $resultFile $numDiscrete $numContinuous $numRows TestSplit.py tsv False
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestSplit.py tsv True
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestPandas.py tsv True
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestPandas.py tsv False
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestRegExQuantifiers.py tsv False
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestRegExQuantifiers.py tsv True
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestRegExTabs.py tsv False
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestRegExTabs.py tsv True
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestMsgPack.py msgpack False
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestMsgPack.py msgpack True
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestFlags.py flag False
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestFlags.py flag True
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestAwk.py tsv False
##  On wide file, mawk gave this type of error so I excluded it: "$32801 exceeds maximum field(32767)"
##  runQuery $resultFile $numDiscrete $numContinuous $numRows TestMawk.py tsv False
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestGawk.py tsv False
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestNawk.py tsv False
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestFixedWidth.py fwf False
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestFixedWidth.py fwf True
}

echo -e "Description\tNumDiscrete\tNumContinuous\tNumRows\tMemMap\tSeconds" > Query_Results.tsv

runQueries Query_Results.tsv 10 90 1000
runQueries Query_Results.tsv 100 900 1000000
runQueries Query_Results.tsv 100000 900000 1000
exit

############################################################
# Build second version of fixed-width files that are more
# compressed and have row and column indices.
############################################################

function buildTestFiles2 {
  numDiscrete=$1
  numContinuous=$2
  numRows=$3

  python3 ConvertTsvToFixedWidthFile2.py TestData/${numDiscrete}_${numContinuous}_${numRows}.tsv TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2
}

#buildTestFiles2 10 90 1000
#buildTestFiles2 100 900 1000000
#buildTestFiles2 100000 900000 1000

############################################################
# Query every 100th column from second version of 
# fixed-width files.
############################################################

function runQueries2 {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4

  dataFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2
  outFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2.tmp

  #echo -e "SelectColumns\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e python3 TestFixedWidth2.py $dataFile $outFile $numRows > /dev/null; } 2>&1 )" >> $resultFile
  time python3 TestFixedWidth2.py $dataFile $outFile $numRows

  masterOutFile=/tmp/TestSplit_${numDiscrete}_${numContinuous}_${numRows}_tsv_False.tsv.out
  python3 CheckOutput.py $outFile $masterOutFile

  rm -f $outFile
}

echo -e "Description\tNumDiscrete\tNumContinuous\tNumRows\tSeconds" > Query_Results_fwf2.tsv

#runQueries2 Query_Results_fwf2.tsv 10 90 1000
#runQueries2 Query_Results_fwf2.tsv 100 900 1000000
#runQueries2 Query_Results_fwf2.tsv 100000 900000 1000

############################################################
# Query second version of fixed-width files. This time 
#   filter rows based on values in 2 columns. Then select
#   every 100th column.
############################################################

function runQueries3 {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4

  dataFile=TestData/${numDiscrete}_${numContinuous}_$numRows.fwf2
  numDataPoints=$(($numDiscrete + $numContinuous))

  echo -e "Filter\t$numDiscrete\t$numContinuous\t$numRows\tTrue\t$( { /usr/bin/time -f %e python3 TestFixedWidth3.py $dataFile /tmp/1 $numRows $numDiscrete $numDataPoints > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 TestFixedWidth3.py $dataFile /tmp/1 $numRows $numDiscrete $numDataPoints
}

#runQueries3 Query_Results_fwf2.tsv 10 90 1000
#runQueries3 Query_Results_fwf2.tsv 100 900 1000000
#runQueries3 Query_Results_fwf2.tsv 100000 900000 1000

############################################################
# Build compressed versions of the second version of fixed-
# width files using a variety of compression algorithms.
# Each line in the data is compressed individually.
############################################################

function compressLines {
  f=$1
  numRows=$2
  method=$3
  level=$4

  echo "Compressing $f with method $method and level $level."

  #echo -e "$f\t$method\t$level\t$( { /usr/bin/time -f %e python3 CompressLines.py $f $numRows $method $level > /dev/null; } 2>&1 )" >> LineCompression_Times.tsv
  python3 CompressLines.py $f $numRows $method $level
}

#echo -e "File\tMethod\tSeconds" > LineCompression_Times.tsv

#for f in TestData/10_*.fwf2
#do
#  compressLines $f 1000 bz2 1
#  compressLines $f 1000 bz2 9
#  compressLines $f 1000 gz 1
#  compressLines $f 1000 gz 9
#  compressLines $f 1000 lzma NA
#  compressLines $f 1000 snappy NA
#done

#for f in TestData/100_*.fwf2
#do
#  compressLines $f 1000000 bz2 1
#  compressLines $f 1000000 bz2 9
#  compressLines $f 1000000 gz 1
#  compressLines $f 1000000 gz 9
#  compressLines $f 1000000 lzma NA
#  compressLines $f 1000000 snappy NA
#done

#for f in TestData/100000_*.fwf2
#do
#  compressLines $f 1000 bz2 1
#  compressLines $f 1000 bz2 9
#  compressLines $f 1000 gz 1
#  compressLines $f 1000 gz 9
#  compressLines $f 1000 lzma NA
#  compressLines $f 1000 snappy NA
#done

############################################################
# Now create compressed versions where we compress the
# entire file (not line by line).
############################################################

function compressFile {
  f=$1

  echo -e "$f\tgz\t$( { /usr/bin/time -f %e gzip $f > /dev/null; } 2>&1 )" >> File_Compression_Times.tsv
}

#echo -e "File\tMethod\tSeconds" > File_Compression_Times.tsv
#for f in TestData/10*.fwf2
#do
#  compressFile $f
#done

############################################################
# Calculate file sizes before and after compression.
############################################################

function calcFileSizes {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4
  extension=$5

  dataFile=TestData/${numDiscrete}_${numContinuous}_$numRows.$extension

  echo -e "$extension\t$numDiscrete\t$numContinuous\t$numRows\t$(python3 PrintFileSize.py $dataFile)" >> $resultFile
}

#sizeFile=File_Sizes.tsv
#echo -e "Extension\tNumDiscrete\tNumContinuous\tNumRows\tSize" > $sizeFile

#for extension in tsv flag msgpack fwf fwf2
#do
#  calcFileSizes $sizeFile 10 90 1000 $extension
#  calcFileSizes $sizeFile 100 900 1000000 $extension
#  calcFileSizes $sizeFile 100000 900000 1000 $extension
#done

function calcFileSizes2 {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4
  method=$5
  level=$6

  dataFile=TestData/${numDiscrete}_${numContinuous}_$numRows.fwf2.$method

  if [[ "$level" != "NA" ]]
  then
    dataFile=${dataFile}_${level}
  fi

  echo -e "$method\t$level\t$numDiscrete\t$numContinuous\t$numRows\t$(python3 PrintFileSize.py $dataFile)" >> $resultFile
}

#sizeFile=Line_Compressed_File_Sizes.tsv
#echo -e "Method\tLevel\tNumDiscrete\tNumContinuous\tNumRows\tSize" > $sizeFile

#calcFileSizes2 $sizeFile 10 90 1000 bz2 1
#calcFileSizes2 $sizeFile 10 90 1000 bz2 9
#calcFileSizes2 $sizeFile 10 90 1000 gz 1
#calcFileSizes2 $sizeFile 10 90 1000 gz 9
#calcFileSizes2 $sizeFile 10 90 1000 lzma NA
#calcFileSizes2 $sizeFile 10 90 1000 snappy NA
#calcFileSizes2 $sizeFile 100 900 1000000 bz2 1
#calcFileSizes2 $sizeFile 100 900 1000000 bz2 9
#calcFileSizes2 $sizeFile 100 900 1000000 gz 1
#calcFileSizes2 $sizeFile 100 900 1000000 gz 9
#calcFileSizes2 $sizeFile 100 900 1000000 lzma NA
#calcFileSizes2 $sizeFile 100 900 1000000 snappy NA
#calcFileSizes2 $sizeFile 100000 900000 1000 bz2 1
#calcFileSizes2 $sizeFile 100000 900000 1000 bz2 9
#calcFileSizes2 $sizeFile 100000 900000 1000 gz 1
#calcFileSizes2 $sizeFile 100000 900000 1000 gz 9
#calcFileSizes2 $sizeFile 100000 900000 1000 lzma NA
#calcFileSizes2 $sizeFile 100000 900000 1000 snappy NA

function calcFileSizes3 {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4
  method=$5

  dataFile=TestData/${numDiscrete}_${numContinuous}_$numRows.fwf2.$method

  echo -e "$method\t$numDiscrete\t$numContinuous\t$numRows\t$(python3 PrintFileSize.py $dataFile)" >> $resultFile
}

#sizeFile=Compressed_File_Sizes.tsv
#echo -e "Method\tNumDiscrete\tNumContinuous\tNumRows\tSize" > $sizeFile

#calcFileSizes3 $sizeFile 10 90 1000 gz
#calcFileSizes3 $sizeFile 100 900 1000000 gz
#calcFileSizes3 $sizeFile 100000 900000 1000 gz

############################################################
# Measure how quickly we can query the files that have
# been compressed line-by-line.
############################################################

function runQueries4 {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4
  compressionMethod=$5
  compressionLevel=$6
  compressionSuffix=$7

  dataFile=TestData/${numDiscrete}_${numContinuous}_$numRows.fwf2.$compressionSuffix
  numDataPoints=$(($numDiscrete + $numContinuous))

  #echo -e "$compressionMethod\t$compressionLevel\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e python3 TestFixedWidth4.py $dataFile /tmp/2 $numRows $numDiscrete $numDataPoints $compressionMethod $compressionLevel > /dev/null; } 2>&1 )" >> $resultFile
############################################
############################################
############################################
# Check this script and then modify commenting
############################################
############################################
############################################
  time python3 TestFixedWidth4.py $dataFile /tmp/2 $numRows $numDiscrete $numDataPoints $compressionMethod $compressionLevel
  wc -l /tmp/2
}

function runAllQueries4 {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4

  runQueries4 $resultFile $numDiscrete $numContinuous $numRows bz2 1 bz2_1
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows bz2 9 bz2_9
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows gz 1 gz_1
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows gz 9 gz_9
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows lzma NA lzma
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows snappy NA snappy
}

#rm -f Query_Results_fwf2_compressed.tsv
#echo -e "Method\tLevel\tNumDiscrete\tNumContinuous\tNumRows\tSeconds" > Query_Results_fwf2_compressed.tsv

#runAllQueries4 Query_Results_fwf2_compressed.tsv 10 90 1000
#runAllQueries4 Query_Results_fwf2_compressed.tsv 100 900 1000000
#runAllQueries4 Query_Results_fwf2_compressed.tsv 100000 900000 1000

############################################################
# Clean up the test files created so far to save disk space.
############################################################

#rm -rfv TestData/*

############################################################
# Build pseudo-genotype files. Measure how long it takes to
# build, query, and transpose these files (of increasing
# size).
############################################################

function runGenotypeTests {
  resultFile=$1
  dimensions=$2

  dataFile=TestData/Genotypes_${dimensions}.fwf2

  echo -e "Build\t$dimensions\t$( { /usr/bin/time -f %e python3 BuildGenotypes.py $dimensions $dataFile > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 BuildGenotypes.py $dimensions $dataFile

  echo -e "Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile)" >> $resultFile
  echo -e "ll Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile.ll)" >> $resultFile
  echo -e "mccl Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile.mccl)" >> $resultFile
  echo -e "cc Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile.cc)" >> $resultFile

  echo -e "Query\t$dimensions\t$( { /usr/bin/time -f %e python3 TestFixedWidth5.py $dataFile $dimensions $dataFile.tmp > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 TestFixedWidth5.py $dataFile $dimensions $dataFile.tmp

  /usr/bin/time -v python3 TransposeFixedWidth.py $dataFile $dimensions $dataFile.tmp 2> /tmp/1
  #time python3 TransposeFixedWidth.py $dataFile $dimensions $dataFile.tmp
  python3 ParseTimeOutput.py /tmp/1 $dimensions >> $resultFile

  echo -e "Transposed Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile.tmp)" >> $resultFile

  rm -f $dataFile ${dataFile}* /tmp/1
}

#resultFile=Results_Genotypes.tsv
#echo -e "Description\tDimensions\tValue" > $resultFile

#runGenotypeTests $resultFile 10
#runGenotypeTests $resultFile 50
#runGenotypeTests $resultFile 100
#runGenotypeTests $resultFile 500
#runGenotypeTests $resultFile 1000
#runGenotypeTests $resultFile 5000
#runGenotypeTests $resultFile 10000
#runGenotypeTests $resultFile 50000
#runGenotypeTests $resultFile 100000
#runGenotypeTests $resultFile 500000

#TODO for Geney and WishBuilder:
#    Exclude sample and feature names (put in meta)
#    Put meta information in the header of the data files so everything is self contained
#    Need/want transposed files?
#      Store transposed version of *compressed* data for filtering and see how much faster it is
#    Support filtering and building pandas dataframe
