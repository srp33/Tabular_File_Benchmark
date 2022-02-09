#!/bin/bash

set -o errexit

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

  if [ ! -f $dataFile ]
  then
    echo Creating $dataFile
    python3 $scriptFile $numDiscrete $numContinuous $numRows $dataFile
  fi
}

function buildTestFiles {
  numDiscrete=$1
  numContinuous=$2
  numRows=$3

  buildTestFile $numDiscrete $numContinuous $numRows BuildTsvFile.py tsv &
  buildTestFile $numDiscrete $numContinuous $numRows BuildMsgPackFile.py msgpack &
  buildTestFile $numDiscrete $numContinuous $numRows BuildFlagFile.py flag &
  wait

  fwfFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf
  if [ ! -f $fwfFile ]
  then
    echo Creating $fwfFile
    python3 ConvertTsvToFixedWidthFile.py TestData/${numDiscrete}_${numContinuous}_${numRows}.tsv $fwfFile
  fi

  # This takes about 16 hours to run...
  #Rscript --vanilla ConvertTsvToRFormats.R TestData/${numDiscrete}_${numContinuous}_${numRows}.tsv TestData/${numDiscrete}_${numContinuous}_${numRows}.fthr TestData/${numDiscrete}_${numContinuous}_${numRows}.fst
  #python3 ConvertTsvToHDF5.py TestData/${numDiscrete}_${numContinuous}_${numRows}.tsv TestData/${numDiscrete}_${numContinuous}_${numRows}.hdf5

  #https://www.danielecook.com/speeding-up-reading-and-writing-in-r/
  #https://data.nozav.org/post/2019-r-data-frame-benchmark/
  #https://vroom.r-lib.org/articles/benchmarks.html
  #fst R package
  #vroom R package
  #arrow?
  #hdf5
}

mkdir -p TestData/TempResults

## Small files
buildTestFiles 10 90 1000
## Tall, narrow files
buildTestFiles 100 900 1000000
## Short, wide files
buildTestFiles 100000 900000 1000

############################################################
# Query every 100th column using a variety of methods.
############################################################

function runQuery {
  resultFile=$1
  fileType=$2
  numDiscrete=$3
  numContinuous=$4
  numRows=$5
  scriptProgram=$6
  scriptFile=$7
  dataFileExtension=$8
  memMap=$9

  scriptName=$(basename $scriptFile)
  scriptName=${scriptName/\.py/}

  echo Testing $scriptFile
  dataFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.$dataFileExtension
  outFile=TestData/TempResults/${scriptName}_${numDiscrete}_${numContinuous}_${numRows}_${dataFileExtension}_${memMap}.$dataFileExtension.out

  rm -f $outFile

  colNamesFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_columns.tsv
  if [[ "$scriptFile" == "TestSplit.py" ]]
  then
    if [ ! -f $colNamesFile ]
    then
      python3 BuildColNamesFile.py $dataFile $colNamesFile
    fi
  fi

  echo -e "$scriptFile\t$fileType\t$numDiscrete\t$numContinuous\t$numRows\t$memMap\t$( { /usr/bin/time -f %e $scriptProgram $scriptFile $dataFile $colNamesFile $outFile $memMap > /dev/null; } 2>&1 )" >> $resultFile
  #time $scriptProgram $scriptFile $dataFile $colNamesFile $outFile $memMap

  masterOutFile=TestData/TempResults/TestSplit_${numDiscrete}_${numContinuous}_${numRows}_tsv_False.tsv.out

  # This compares against the output using the "ParseSplit" method
  if [[ "$scriptFile" != "TestSplit.py" ]]
  then
    echo Checking $outFile
    python3 CheckOutput.py $outFile $masterOutFile
  fi
}

function runQueries {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4

  runQuery $resultFile text $numDiscrete $numContinuous $numRows python3 TestSplit.py tsv False
  runQuery $resultFile text $numDiscrete $numContinuous $numRows python3 TestSplit.py tsv True
  runQuery $resultFile text $numDiscrete $numContinuous $numRows python3 TestPandas.py tsv True
  runQuery $resultFile text $numDiscrete $numContinuous $numRows python3 TestAwk.py tsv False
####  On wide file, mawk gave this type of error so I excluded it: "$32801 exceeds maximum field(32767)"
####  runQuery $resultFile text $numDiscrete $numContinuous $numRows python3 TestMawk.py tsv False
  runQuery $resultFile text $numDiscrete $numContinuous $numRows python3 TestGawk.py tsv False
  runQuery $resultFile text $numDiscrete $numContinuous $numRows python3 TestNawk.py tsv False
  runQuery $resultFile text $numDiscrete $numContinuous $numRows python3 TestCut.py tsv False
  runQuery $resultFile text $numDiscrete $numContinuous $numRows python3 TestFixedWidth.py fwf False
  runQuery $resultFile text $numDiscrete $numContinuous $numRows python3 TestFixedWidth.py fwf True

#### Not really supported: see comments in TestReadTsv.R.
####  runQuery $resultFile text $numDiscrete $numContinuous $numRows "Rscript --vanilla" TestReadTsv.R tsv False
#### This is very fast on the tall TSV file. It throws a SegFault on the wide TSV file.
####  runQuery $resultFile text $numDiscrete $numContinuous $numRows "Rscript --vanilla" TestFread.R tsv False
#  runQuery $resultFile binary $numDiscrete $numContinuous $numRows "Rscript --vanilla" TestFeather.R fthr False
#  runQuery $resultFile binary $numDiscrete $numContinuous $numRows "Rscript --vanilla" TestFst.R fst False
#  runQuery $resultFile binary $numDiscrete $numContinuous $numRows python3 TestHDF5.py hdf5 False
}

resultFile=Results2/Query_Results.tsv

if [ ! -f $resultFile ]
then
  echo -e "Description\tFileType\tNumDiscrete\tNumContinuous\tNumRows\tMemMap\tSeconds" > $resultFile
  runQueries $resultFile 10 90 1000
  runQueries $resultFile 100 900 1000000
  runQueries $resultFile 100000 900000 1000
fi

############################################################
# Build second version of fixed-width files that are more
# compressed and have row and column indices.
############################################################

function buildTestFiles2 {
  numDiscrete=$1
  numContinuous=$2
  numRows=$3

  outFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2

  if [ ! -f $outFile ]
  then
    python3 ConvertTsvToFixedWidthFile2.py TestData/${numDiscrete}_${numContinuous}_${numRows}.tsv $outFile
  fi
}

buildTestFiles2 10 90 1000 &
buildTestFiles2 100 900 1000000 &
buildTestFiles2 100000 900000 1000 &
wait

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
  llFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2.ll
  ccFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2.cc
  mcclFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2.mccl
  colNamesFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_columns.tsv
  outFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2.tmp

  # Python FWF2 with memory mapping
  rm -f $outFile
  output=$({ /usr/bin/time --verbose python3 TestFixedWidth2_Updated.py $dataFile $colNamesFile $outFile $numRows MMAP 2>&1; } | python3 ParseTimeMemoryInfo.py)
  echo -e "Python\tYes\tSelectColumns\t$numDiscrete\t$numContinuous\t$numRows\t$output" >> $resultFile
  masterOutFile=TestData/TempResults/TestSplit_${numDiscrete}_${numContinuous}_${numRows}_tsv_False.tsv.out
  python3 CheckOutput.py $outFile $masterOutFile

  # Python FWF2 without memory mapping
  rm -f $outFile
  output=$({ /usr/bin/time --verbose python3 TestFixedWidth2_Updated.py $dataFile $colNamesFile $outFile $numRows NO_MMAP 2>&1; } | python3 ParseTimeMemoryInfo.py)
  echo -e "Python\tNo\tSelectColumns\t$numDiscrete\t$numContinuous\t$numRows\t$output" >> $resultFile
  python3 CheckOutput.py $outFile $masterOutFile

  # C++ with memory mapping
  rm -f $outFile
  output=$({ /usr/bin/time --verbose ./TestFixedWidth2 $llFile $dataFile $ccFile $outFile $mcclFile $colNamesFile $numRows MMAP 2>&1; } | python3 ParseTimeMemoryInfo.py)
  echo -e "C++\tYes\tSelectColumns\t$numDiscrete\t$numContinuous\t$numRows\t$output" >> $resultFile
  python3 CheckOutput.py $outFile $masterOutFile

  # C++ without memory mapping
  rm -f $outFile
  output=$({ /usr/bin/time --verbose ./TestFixedWidth2 $llFile $dataFile $ccFile $outFile $mcclFile $colNamesFile $numRows NO_MMAP 2>&1; } | python3 ParseTimeMemoryInfo.py)
  echo -e "C++\tNo\tSelectColumns\t$numDiscrete\t$numContinuous\t$numRows\t$output" >> $resultFile
  python3 CheckOutput.py $outFile $masterOutFile

  # Add Rust without memory mapping
  rm -f $outFile
  output=$({ /usr/bin/time --verbose /Rust/TestFixedWidth2/target/release/main $llFile $dataFile $ccFile $outFile $mcclFile $colNamesFile $numRows NO_MMAP 2>&1; } | python3 ParseTimeMemoryInfo.py)
  echo -e "Rust\tNo\tSelectColumns\t$numDiscrete\t$numContinuous\t$numRows\t$output" >> $resultFile
  python3 CheckOutput.py $outFile $masterOutFile

  # Rust with memory mapping
  rm -f $outFile
  output=$({ /usr/bin/time --verbose /Rust/TestFixedWidth2/target/release/main $llFile $dataFile $ccFile $outFile $mcclFile $colNamesFile $numRows MMAP 2>&1; } | python3 ParseTimeMemoryInfo.py)
  echo -e "Rust\tYes\tSelectColumns\t$numDiscrete\t$numContinuous\t$numRows\t$output" >> $resultFile
  python3 CheckOutput.py $outFile $masterOutFile
}

resultFile=Results2/Query_Results_fwf2.tsv

#if [ !  -f $resultFile ]
#then
  echo -e "Language\tMemMapping\tDescription\tNumDiscrete\tNumContinuous\tNumRows\tWallClockSeconds\tMaxMemoryKilobytes" > $resultFile

  runQueries2 $resultFile 10 90 1000
  runQueries2 $resultFile 100 900 1000000
  runQueries2 $resultFile 100000 900000 1000
#fi
#TODO: detached mode?
echo "got here"
exit

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

  dataFilePrefix=TestData/${numDiscrete}_${numContinuous}_$numRows
  ctFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2.ct
  numDataPoints=$(($numDiscrete + $numContinuous))
  colNamesFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_columns.tsv
  masterOutFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_queries3_master.tsv

  dataFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2
  outFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2.tmp
  llFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2.ll
  ccFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2.cc
  mcclFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2.mccl
  colNamesFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_columns.tsv
  outFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_queries3.fwf2

  # Set baseline using tsv file
  rm -f $masterOutFile
  rm -f $outFile
  echo -e "Filter\ttsv\tPython\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e python3 TestSplitFilter.py $dataFilePrefix.tsv $colNamesFile $masterOutFile $numDiscrete $numDataPoints > /dev/null; } 2>&1 )" >> $resultFile

  rm -f $outFile
  echo -e "Filter\tfwf2\tPython\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e python3 TestFixedWidth3.py $dataFilePrefix.fwf2 $colNamesFile $outFile $numRows $numDiscrete,$numDataPoints > /dev/null; } 2>&1 )" >> $resultFile
  python3 CheckOutput.py $outFile $masterOutFile

  #rm -f $outFile
  #echo -e "Filter\tfthr\tR\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e Rscript --vanilla TestFeatherFilter.R $dataFilePrefix.fthr $colNamesFile $outFile $numDiscrete $numDataPoints > /dev/null; } 2>&1 )" >> $resultFile
  #time Rscript --vanilla TestFeatherFilter.R $dataFilePrefix.fthr $colNamesFile $outFile $numDiscrete $numDataPoints
  #python3 CheckOutput.py $outFile $masterOutFile
  
  rm -f $outFile
  echo -e "Filter\tfwf2\tC++\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e ./TestFixedWidth3 $llFile $dataFile $ccFile $outFile $mcclFile $colNamesFile $numRows $ctFile $numDiscrete,$numDataPoints> /dev/null; } 2>&1 )" >> $resultFile
  python3 CheckOutput.py $outFile $masterOutFile

  rm -f $outFile
  echo -e "Filter\tfwf2\tRust\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e /Rust/TestFixedWidth3/target/release/main $dataFile $colNamesFile $outFile $numRows $numDiscrete,$numDataPoints > /dev/null; } 2>&1 )" >> $resultFile
  python3 CheckOutput.py $outFile $masterOutFile
}

resultFile=Results2/Query_Results_Filtering.tsv

if [ ! -f $resultFile ]
then
  echo -e "Description\tFormat\tLanguage\tNumDiscrete\tNumContinuous\tNumRows\tWallClockSeconds\tMaxMemoryUsed" > $resultFile

  runQueries3 $resultFile 10 90 1000
  runQueries3 $resultFile 100 900 1000000
  runQueries3 $resultFile 100000 900000 1000
fi

############################################################
# Build compressed versions of the second version of fixed-
# width files using a variety of compression algorithms.
# Each line in the data is compressed individually.
############################################################

function compressLines {
  resultFile=$1
  f=$2
  numRows=$3
  method=$4
  level=$5

  echo "Compressing $f with method $method and level $level."

  echo -e "$f\t$method\t$level\t$( { /usr/bin/time -f %e python3 CompressLines.py $f $numRows $method $level False > /dev/null; } 2>&1 )" >> $resultFile
#  python3 CompressLines.py $f $numRows $method $level False
}

resultFile=Results2/Line_Compression_Times.tsv

if [ ! -f $resultFile ]
then
  echo -e "File\tMethod\tLevel\tSeconds" > $resultFile

  for f in TestData/10_*.fwf2 TestData/100000_*.fwf2
  do
    compressLines $resultFile $f 1000 bz2 1
    compressLines $resultFile $f 1000 bz2 5
    compressLines $resultFile $f 1000 bz2 9
    compressLines $resultFile $f 1000 gz 1
    compressLines $resultFile $f 1000 gz 5
    compressLines $resultFile $f 1000 gz 9
    compressLines $resultFile $f 1000 lzma NA
    compressLines $resultFile $f 1000 snappy NA
    compressLines $resultFile $f 1000 zstd 1
    compressLines $resultFile $f 1000 zstd 5
    compressLines $resultFile $f 1000 zstd 9
    compressLines $resultFile $f 1000 zstd 13
    compressLines $resultFile $f 1000 zstd 17
    compressLines $resultFile $f 1000 zstd 22
    compressLines $resultFile $f 1000 lz4 0
    compressLines $resultFile $f 1000 lz4 4
    compressLines $resultFile $f 1000 lz4 8
    compressLines $resultFile $f 1000 lz4 12
    compressLines $resultFile $f 1000 lz4 16
  done

  for f in TestData/100_*.fwf2
  do
    compressLines $resultFile $f 1000000 bz2 1
    compressLines $resultFile $f 1000000 bz2 5
    compressLines $resultFile $f 1000000 bz2 9
    compressLines $resultFile $f 1000000 gz 1
    compressLines $resultFile $f 1000000 gz 5
    compressLines $resultFile $f 1000000 gz 9
    compressLines $resultFile $f 1000000 lzma NA
    compressLines $resultFile $f 1000000 snappy NA
    compressLines $resultFile $f 1000000 zstd 1
    compressLines $resultFile $f 1000000 zstd 5
    compressLines $resultFile $f 1000000 zstd 9
    compressLines $resultFile $f 1000000 zstd 13
    compressLines $resultFile $f 1000000 zstd 17
    compressLines $resultFile $f 1000000 zstd 22
    compressLines $resultFile $f 1000000 lz4 0
    compressLines $resultFile $f 1000000 lz4 4
    compressLines $resultFile $f 1000000 lz4 8
    compressLines $resultFile $f 1000000 lz4 12
    compressLines $resultFile $f 1000000 lz4 16
  done
fi

############################################################
# Now create compressed versions where we compress the
# entire file (not line by line). This uses gzip (level 9)
# only.
############################################################

for f in TestData/10*.fwf2
do
  if [ ! -f $f.gz ]
  then
    gzip -k -f $f &
  fi
done
wait

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

sizeFile=Results2/Uncompressed_File_Sizes.tsv

if [ ! -f $sizeFile ]
then
  echo -e "Extension\tNumDiscrete\tNumContinuous\tNumRows\tSize" > $sizeFile

  for extension in tsv flag msgpack fwf fwf2
  do
    calcFileSizes $sizeFile 10 90 1000 $extension
    calcFileSizes $sizeFile 100 900 1000000 $extension
    calcFileSizes $sizeFile 100000 900000 1000 $extension
  done
fi

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

sizeFile=Results2/Line_Compressed_File_Sizes.tsv

if [ ! -f $sizeFile ]
then
  echo -e "Method\tLevel\tNumDiscrete\tNumContinuous\tNumRows\tSize" > $sizeFile

  calcFileSizes2 $sizeFile 10 90 1000 bz2 1
  calcFileSizes2 $sizeFile 10 90 1000 bz2 5
  calcFileSizes2 $sizeFile 10 90 1000 bz2 9
  calcFileSizes2 $sizeFile 10 90 1000 gz 1
  calcFileSizes2 $sizeFile 10 90 1000 gz 5
  calcFileSizes2 $sizeFile 10 90 1000 gz 9
  calcFileSizes2 $sizeFile 10 90 1000 lzma NA
  calcFileSizes2 $sizeFile 10 90 1000 snappy NA
  calcFileSizes2 $sizeFile 10 90 1000 zstd 1
  calcFileSizes2 $sizeFile 10 90 1000 zstd 5
  calcFileSizes2 $sizeFile 10 90 1000 zstd 9
  calcFileSizes2 $sizeFile 10 90 1000 zstd 13
  calcFileSizes2 $sizeFile 10 90 1000 zstd 17
  calcFileSizes2 $sizeFile 10 90 1000 zstd 22
  calcFileSizes2 $sizeFile 10 90 1000 lz4 0
  calcFileSizes2 $sizeFile 10 90 1000 lz4 4
  calcFileSizes2 $sizeFile 10 90 1000 lz4 8
  calcFileSizes2 $sizeFile 10 90 1000 lz4 12
  calcFileSizes2 $sizeFile 10 90 1000 lz4 16
  calcFileSizes2 $sizeFile 100 900 1000000 bz2 1
  calcFileSizes2 $sizeFile 100 900 1000000 bz2 5
  calcFileSizes2 $sizeFile 100 900 1000000 bz2 9
  calcFileSizes2 $sizeFile 100 900 1000000 gz 1
  calcFileSizes2 $sizeFile 100 900 1000000 gz 5
  calcFileSizes2 $sizeFile 100 900 1000000 gz 9
  calcFileSizes2 $sizeFile 100 900 1000000 lzma NA
  calcFileSizes2 $sizeFile 100 900 1000000 snappy NA
  calcFileSizes2 $sizeFile 100 900 1000000 zstd 1
  calcFileSizes2 $sizeFile 100 900 1000000 zstd 5
  calcFileSizes2 $sizeFile 100 900 1000000 zstd 9
  calcFileSizes2 $sizeFile 100 900 1000000 zstd 13
  calcFileSizes2 $sizeFile 100 900 1000000 zstd 17
  calcFileSizes2 $sizeFile 100 900 1000000 zstd 22
  calcFileSizes2 $sizeFile 100 900 1000000 lz4 0
  calcFileSizes2 $sizeFile 100 900 1000000 lz4 4
  calcFileSizes2 $sizeFile 100 900 1000000 lz4 8
  calcFileSizes2 $sizeFile 100 900 1000000 lz4 12
  calcFileSizes2 $sizeFile 100 900 1000000 lz4 16
  calcFileSizes2 $sizeFile 100000 900000 1000 bz2 1
  calcFileSizes2 $sizeFile 100000 900000 1000 bz2 5
  calcFileSizes2 $sizeFile 100000 900000 1000 bz2 9
  calcFileSizes2 $sizeFile 100000 900000 1000 gz 1
  calcFileSizes2 $sizeFile 100000 900000 1000 gz 5
  calcFileSizes2 $sizeFile 100000 900000 1000 gz 9
  calcFileSizes2 $sizeFile 100000 900000 1000 lzma NA
  calcFileSizes2 $sizeFile 100000 900000 1000 snappy NA
  calcFileSizes2 $sizeFile 100000 900000 1000 zstd 1
  calcFileSizes2 $sizeFile 100000 900000 1000 zstd 5
  calcFileSizes2 $sizeFile 100000 900000 1000 zstd 9
  calcFileSizes2 $sizeFile 100000 900000 1000 zstd 13
  calcFileSizes2 $sizeFile 100000 900000 1000 zstd 17
  calcFileSizes2 $sizeFile 100000 900000 1000 zstd 22
  calcFileSizes2 $sizeFile 100000 900000 1000 lz4 0
  calcFileSizes2 $sizeFile 100000 900000 1000 lz4 4
  calcFileSizes2 $sizeFile 100000 900000 1000 lz4 8
  calcFileSizes2 $sizeFile 100000 900000 1000 lz4 12
  calcFileSizes2 $sizeFile 100000 900000 1000 lz4 16
fi

function calcFileSizes3 {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4
  method=$5

  dataFile=TestData/${numDiscrete}_${numContinuous}_$numRows.fwf2.$method

  echo -e "$method\t$numDiscrete\t$numContinuous\t$numRows\t$(python3 PrintFileSize.py $dataFile)" >> $resultFile
}

sizeFile=Results2/WholeFile_Compressed_File_Sizes.tsv

if [ ! -f $sizeFile ]
then
  echo -e "Method\tNumDiscrete\tNumContinuous\tNumRows\tSize" > $sizeFile

  calcFileSizes3 $sizeFile 10 90 1000 gz
  calcFileSizes3 $sizeFile 100 900 1000000 gz
  calcFileSizes3 $sizeFile 100000 900000 1000 gz
fi

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
  colNamesFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_columns.tsv
  masterOutFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_queries3_master.tsv
  outFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_queries3.$compressionSuffix

  rm -f $outFile

  echo -e "$compressionMethod\t$compressionLevel\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e python3 TestFixedWidth4.py $dataFile $colNamesFile $outFile $numRows $numDiscrete,$numDataPoints $compressionMethod $compressionLevel > /dev/null; } 2>&1 )" >> $resultFile
  #python3 TestFixedWidth4.py $dataFile $colNamesFile $outFile $numRows $numDiscrete,$numDataPoints $compressionMethod $compressionLevel

  # We are not using this version in our tests, but it does work (for zstd=1).
  #time /Rust/TestFixedWidth4/target/release/main $dataFile $colNamesFile $outFile $numRows $numDiscrete,$numDataPoints $compressionMethod

  python3 CheckOutput.py $outFile $masterOutFile
}

function runAllQueries4 {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4

  runQueries4 $resultFile $numDiscrete $numContinuous $numRows bz2 1 bz2_1
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows bz2 5 bz2_5
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows bz2 9 bz2_9
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows gz 1 gz_1
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows gz 5 gz_5
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows gz 9 gz_9
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows lzma NA lzma
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows snappy NA snappy
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows zstd 1 zstd_1
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows zstd 5 zstd_5
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows zstd 9 zstd_9
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows zstd 13 zstd_13
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows zstd 17 zstd_17
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows zstd 22 zstd_22
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows lz4 0 lz4_0
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows lz4 4 lz4_4
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows lz4 8 lz4_8
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows lz4 12 lz4_12
  runQueries4 $resultFile $numDiscrete $numContinuous $numRows lz4 16 lz4_16
}

resultFile=Results2/Query_Results_fwf2_compressed.tsv

if [ ! -f $resultFile ]
then
  echo -e "Method\tLevel\tNumDiscrete\tNumContinuous\tNumRows\tSeconds" > $resultFile

  runAllQueries4 $resultFile 10 90 1000
  runAllQueries4 $resultFile 100 900 1000000
  runAllQueries4 $resultFile 100000 900000 1000
fi

############################################################
# Measure how quickly we can query the files that have
# been compressed line-by-line. This time use the transposed
# versions of the files in addition to the non-transposed
# versions.
############################################################

function transposeCompressTestFile {
  sizeFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4
  numRowsTransposed=$5

  mkdir -p TestData/Transposed

  dataFile=TestData/${numDiscrete}_${numContinuous}_${numRows}.fwf2
  transposedFile=TestData/Transposed/${numDiscrete}_${numContinuous}_${numRows}.fwf2

  echo Transposing $dataFile to $transposedFile
  python3 TransposeFixedWidth.py $dataFile $transposedFile
  echo -e "Uncompressed\t$numDiscrete\t$numContinuous\t$numRows\t$(python3 PrintFileSize.py $transposedFile)" >> $sizeFile

  method=zstd

  for level in 1 5 9 13 17 22
  do
    echo Compressing $transposed file using $method and $level
    python3 CompressLines.py $transposedFile $numRowsTransposed $method $level True

    file1=$(python3 PrintFileSize.py $dataFile.${method}_${level})
    file2=$(python3 PrintFileSize.py $transposedFile.${method}_${level})
    totalSize=$((file1 + file2))
    echo -e "${method}_${level}\t$numDiscrete\t$numContinuous\t$numRows\t$totalSize" >> $sizeFile
  done
}

sizeFile=Results2/File_Sizes_transposed.tsv

if [ ! -f $sizeFile ]
then
  echo -e "Description\tNumDiscrete\tNumContinuous\tNumRows\tSize" > $sizeFile

  transposeCompressTestFile $sizeFile 10 90 1000 100
  transposeCompressTestFile $sizeFile 100 900 1000000 1000
  transposeCompressTestFile $sizeFile 100000 900000 1000 1000000
fi

function runQuery4T {
  resultFile=$1
  numDiscrete=$2
  numContinuous=$3
  numRows=$4
  compressionMethod=$5
  compressionLevel=$6
  compressionSuffix=$7

  dataFile=TestData/${numDiscrete}_${numContinuous}_$numRows.fwf2.$compressionSuffix
  transposedFile=TestData/Transposed/${numDiscrete}_${numContinuous}_$numRows.fwf2.$compressionSuffix
  transposedFileC=TestData/Transposed/${numDiscrete}_${numContinuous}_$numRows.fwf2
  numDataPoints=$(($numDiscrete + $numContinuous))
  colNamesFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_columns.tsv
  masterOutFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_queries3_master.tsv
  outFile=TestData/TempResults/${numDiscrete}_${numContinuous}_${numRows}_queries4.$compressionSuffix

  echo Query4T - $compressionMethod - $compressionLevel - $numDiscrete - $numContinuous - $numRows
  
  rm -f $outFile
  echo -e "$compressionMethod\t$compressionLevel\tPython\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e python3 TestFixedWidth4T.py $dataFile $transposedFile $colNamesFile $outFile $numDiscrete,$numDataPoints $compressionMethod $compressionLevel > /dev/null; } 2>&1 )" >> $resultFile
  #python3 TestFixedWidth4T.py $dataFile $transposedFile $colNamesFile $outFile $numDiscrete,$numDataPoints $compressionMethod $compressionLevel
  python3 CheckOutput.py $outFile $masterOutFile

  ## I am getting a segmentation fault sometimes, so leave this out.
  #rm -f $outFile
  #echo -e "$compressionMethod\t$compressionLevel\tC++\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e ./TestFixedWidth4T $dataFile $transposedFile $colNamesFile $outFile $numDiscrete,$numDataPoints > /dev/null; } 2>&1 )" >> $resultFile
  #./TestFixedWidth4T $dataFile $transposedFile $colNamesFile $outFile $numDiscrete,$numDataPoints
  #python3 CheckOutput.py $outFile $masterOutFile
  
  rm -f $outFile
  echo -e "$compressionMethod\t$compressionLevel\tRust\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e /Rust/TestFixedWidth4T/target/release/main $dataFile $transposedFile $colNamesFile $outFile $numDiscrete,$numDataPoints > /dev/null; } 2>&1 )" >> $resultFile
  #/Rust/TestFixedWidth4T/target/release/main $dataFile $transposedFile $colNamesFile $outFile $numDiscrete,$numDataPoints
  python3 CheckOutput.py $outFile $masterOutFile
}

resultFile=Results2/Query_Results_fwf2_compressed_transposed.tsv

if [ ! -f $resultFile ]
then
  echo -e "Method\tLevel\tLanguage\tNumDiscrete\tNumContinuous\tNumRows\tSeconds" > $resultFile

  for level in 1 5 9 13 17 22
  do
    runQuery4T $resultFile 10 90 1000 zstd ${level} zstd_${level}
    runQuery4T $resultFile 100 900 1000000 zstd ${level} zstd_${level}
    runQuery4T $resultFile 100000 900000 1000 zstd ${level} zstd_${level}
  done
fi

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
  rowIndicesFile=TestData/Genotypes_${dimensions}.ri
  colIndicesFile=TestData/Genotypes_${dimensions}.ci

  echo -e "Build\t$dimensions\t$( { /usr/bin/time -f %e python3 BuildGenotypes.py $dimensions $dataFile > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 BuildGenotypes.py $dimensions $dataFile

  echo -e "Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile)" >> $resultFile
  echo -e "ll Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile.ll)" >> $resultFile
  echo -e "mccl Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile.mccl)" >> $resultFile
  echo -e "cc Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile.cc)" >> $resultFile

  # Build a file that indicates which column indices to query before performing the actual query.
  echo -e "Build Row Indices\t$dimensions\t$( { /usr/bin/time -f %e python3 BuildRandomIndicesFile.py $dimensions 1 $rowIndicesFile > /dev/null; } 2>&1 )" >> $resultFile
  echo -e "Build Column Indices\t$dimensions\t$( { /usr/bin/time -f %e python3 BuildRandomIndicesFile.py $dimensions 2 $colIndicesFile > /dev/null; } 2>&1 )" >> $resultFile

  echo -e "Query\t$dimensions\t$( { /usr/bin/time -f %e python3 TestFixedWidth5.py $dataFile $rowIndicesFile $colIndicesFile $dataFile.tmp > /dev/null; } 2>&1 )" >> $resultFile
  #time python3 TestFixedWidth5.py $dataFile $rowIndicesFile $colIndicesFile $dataFile.tmp

  /usr/bin/time -v python3 TransposeFixedWidthGenotypes.py $dataFile $dimensions $dataFile.tmp 2> TestData/TempResults/1
  #time python3 TransposeFixedWidthGenotypes.py $dataFile $dimensions $dataFile.tmp
  python3 ParseTimeOutput.py TestData/TempResults/1 $dimensions >> $resultFile

  echo -e "Transposed Size\t$dimensions\t$(python3 PrintFileSize.py $dataFile.tmp)" >> $resultFile

  rm -f $dataFile ${dataFile}* TestData/TempResults/1
}

resultFile=Results2/Results_Genotypes.tsv

#if [ ! -f $resultFile ]
#then
  echo -e "Description\tDimensions\tValue" > $resultFile

  runGenotypeTests $resultFile 10
  runGenotypeTests $resultFile 50
  runGenotypeTests $resultFile 100
  runGenotypeTests $resultFile 500
  runGenotypeTests $resultFile 1000
  runGenotypeTests $resultFile 5000
  runGenotypeTests $resultFile 10000
  runGenotypeTests $resultFile 50000
  runGenotypeTests $resultFile 100000
  runGenotypeTests $resultFile 500000
#fi

#TODO: detached mode?
echo "got here"
exit

############################################################
# Download, parse, and query gnomad files.
############################################################

#wget https://storage.googleapis.com/gnomad-public/release/2.1.1/vcf/genomes/gnomad.genomes.r2.1.1.sites.vcf.bgz

#TODO: Copy stuff from gnomad.sh, cadd.sh

#git clone https://github.com/srp33/F4.git
