#!/bin/bash

set -o errexit

############################################################
# Prep and clean before beginning analysis.
############################################################

#rm -rfv TestData
#mkdir -pv TestData

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

#time buildTestFiles 10 90 1000
## A tall, narrow file
#time buildTestFiles 100 900 1000000
## A short, wide file
#time buildTestFiles 100000 900000 1000

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

#echo -e "Description\tNumDiscrete\tNumContinuous\tNumRows\tMemMap\tSeconds" > Query_Results.tsv

#runQueries Query_Results.tsv 10 90 1000
#runQueries Query_Results.tsv 100 900 1000000
#runQueries Query_Results.tsv 100000 900000 1000

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

  ###runQuery $resultFile $numDiscrete $numContinuous $numRows TestSplit.py tsv True
  runQuery $resultFile $numDiscrete $numContinuous $numRows TestFixedWidth2.py fwf2 True
}

#echo -e "Description\tNumDiscrete\tNumContinuous\tNumRows\tMemMap\tSeconds" > Query_Results_fwf2.tsv

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

  echo -e "Filter\t$numDiscrete\t$numContinuous\t$numRows\tTrue\t$( { /usr/bin/time -f %e python3 TestFixedWidth3.py $dataFile /tmp/1 $numDiscrete $numDataPoints > /dev/null; } 2>&1 )" >> $resultFile
}

#runQueries3 Query_Results_fwf2.tsv 10 90 1000
#runQueries3 Query_Results_fwf2.tsv 100 900 1000000
#runQueries3 Query_Results_fwf2.tsv 100000 900000 1000

############################################################
# Build compressed versions of the second version of fixed-
# width files using a variety of compression algorithms.
# each row in the data is compressed individually.
############################################################

function compressFile {
  f=$1
  method=$2
  level=$3

  echo "Compressing $f with method $method and level $level."

  echo -e "$f\t$method\t$level\t$( { /usr/bin/time -f %e python3 CompressLines.py $f $method $level > /dev/null; } 2>&1 )" >> Compression_Times.tsv
  ####python3 CompressLines.py $f $method $level
}

#echo -e "File\tMethod\tSeconds" > Compression_Times.tsv

#for f in TestData/*.fwf2
#do
#  compressFile $f bz2 1
#  compressFile $f bz2 9
#  compressFile $f gz 1
#  compressFile $f gz 9
#  compressFile $f lzma NA
#  compressFile $f snappy NA
#done

############################################################
# Measure how quickly we can query from each type of
# compressed file.
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

  echo -e "$compressionMethod\t$compressionLevel\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e python3 TestFixedWidth4.py $dataFile /tmp/2 $numDiscrete $numDataPoints $compressionMethod $compressionLevel > /dev/null; } 2>&1 )" >> $resultFile
  #python3 TestFixedWidth4.py $dataFile /tmp/2 $numDiscrete $numDataPoints $compressionMethod $compressionLevel
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

#rm -rfv TestData

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
  #python3 PrintFileSize.py $dataFile

  echo -e "Query\t$dimensions\t$( { /usr/bin/time -f %e python3 TestFixedWidth5.py $dataFile $dataFile.tmp > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 TestFixedWidth5.py $dataFile $dataFile.tmp

  echo -e "Transpose\t$dimensions\t$( { /usr/bin/time -f %e python3 TransposeFixedWidth.py $dataFile $dataFile.tmp > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 TransposeFixedWidth.py $dataFile $dataFile.tmp

  echo -e "Transposed Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile.tmp)" >> $resultFile

  rm -f $dataFile ${dataFile}*
}

echo -e "Description\tDimensions\tSeconds" > Results_Genotypes.tsv

runGenotypeTests Results_Genotypes.tsv 10
runGenotypeTests Results_Genotypes.tsv 50
runGenotypeTests Results_Genotypes.tsv 100
runGenotypeTests Results_Genotypes.tsv 500
runGenotypeTests Results_Genotypes.tsv 1000
runGenotypeTests Results_Genotypes.tsv 5000
runGenotypeTests Results_Genotypes.tsv 10000
runGenotypeTests Results_Genotypes.tsv 50000
runGenotypeTests Results_Genotypes.tsv 100000
#runGenotypeTests Results_Genotypes.tsv 500000
#runGenotypeTests Results_Genotypes.tsv 1000000


#TODO for paper:
#  Save file sizes (in a TSV file):
#    For each initial file format
#    After each type of compression
#  GCTX?
#    https://github.com/cmap/cmapPy/blob/master/tutorials/cmapPy_pandasGEXpress_tutorial.ipynb

#TODO for Geney and WishBuilder:
#    Store transposed version of *compressed* data for filtering and see how much faster it is
#    Exclude sample and feature names?
#    Support filtering and building pandas dataframe
#    Need/want transposed files?
#    Store dictionaries in sqlitedict?
#      Test whether it's faster to store as msgpack or in key/value database

#NOTES:
#  Monitor memory usage?: https://stackoverflow.com/questions/774556/peak-memory-usage-of-a-linux-unix-process
