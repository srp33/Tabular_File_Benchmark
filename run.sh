#! /bin/bash

#set -o errexit

#######################################################
# Set up Docker
#######################################################

pythonImage=tab_bench_python
rImage=tab_bench_r

for dockerFile in Dockerfiles/tab_bench_*
do
    docker build -t $(basename $dockerFile) -f $dockerFile .
done

baseDockerCommand="docker run -i -t --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox"
#baseDockerCommand="docker run -d --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox"
pythonDockerCommand="$baseDockerCommand $pythonImage"
rDockerCommand="$baseDockerCommand $rImage"

#######################################################
# Create TSV files
#######################################################

mkdir -p data

## Small file
#$pythonDockerCommand python /sandbox/BuildTsvFile.py 10 90 1000 /data/10_90_1000.tsv
## Tall, narrow file
#$pythonDockerCommand python /sandbox/BuildTsvFile.py 100 900 1000000 /data/100_900_1000000.tsv
## Short, wide file
#$pythonDockerCommand python /sandbox/BuildTsvFile.py 100000 900000 1000 /data/100000_900000_1000.tsv

#######################################################
# Query TSV files. Filter based on values in 2 columns.
#   Then select other columns.
#######################################################

function queryTSV {
  numDiscrete=$1
  numNumeric=$2
  numRows=$3
  dockerCommand="$4"
  commandPrefix="$5"
  queryType=$6
  isMaster=$7
  resultFile=$8

  dataFile=data/${numDiscrete}_${numNumeric}_$numRows.tsv
  outFile=/tmp/tsv_tests/${numDiscrete}_${numNumeric}_${numRows}_${queryType}.tsv

  rm -f $outFile

  echo -n -e "${commandPrefix}\t$numDiscrete\t$numNumeric\t$numRows\t" >> $resultFile
  
  command="${commandPrefix} $queryType $dataFile $outFile Discrete2 Numeric2 Discrete1,Discrete${numDiscrete},Numeric1,Numeric${numNumeric}"

  $dockerCommand $command
#  $dockerCommand /usr/bin/time --verbose $command &> /tmp/result
#  $pythonDockerCommand python ParseTimeMemoryInfo.py /tmp/result >> $resultFile
#  echo >> $resultFile

  masterFile=/tmp/tsv_tests/${numDiscrete}_${numNumeric}_${numRows}_${queryType}_master.tsv

  if [[ "$isMaster" == "False" ]]
  then
      if [ -f $outFile ]
      then
          echo Checking output for ${numDiscrete}, ${numNumeric}, ${numRows}, ${commandPrefix}, ${queryType}
          python CheckOutput.py $outFile $masterFile
      else
          echo No output for ${numDiscrete}, ${numNumeric}, ${numRows}, ${commandPrefix}, ${queryType}
      fi
  else
    echo Saving master file for ${numDiscrete}, ${numNumeric}, ${numRows}, ${commandPrefix}, ${queryType}
    mv $outFile $masterFile
    echo "  Done"
  fi
}

#rm -rf /tmp/tsv_tests
mkdir -p /tmp/tsv_tests

mkdir -p results
resultFile=results/tsv_queries.tsv

echo -e "CommandPrefix\tNumDiscrete\tNumNumeric\tNumRows\tWallClockSeconds\tUserSeconds\tSystemSeconds\tMaxMemoryUsed" > $resultFile

small="10 90 1000"
tall="100 900 1000000"
wide="100000 900000 1000"

#for queryType in simple startsendswith
#for queryType in simple
#for queryType in startsendswith
#do
#    for size in "$small" "$tall" "$wide"
#    for size in "$small"
#    for size in "$tall"
#    do
#        queryTSV $size "${pythonDockerCommand}" "python line_by_line.py standard_io" $queryType True $resultFile
#        queryTSV $size "${pythonDockerCommand}" "python line_by_line.py memory_map" $queryType False $resultFile
#        queryTSV $size "${pythonDockerCommand}" "python awk.py awk" $queryType False $resultFile
#        queryTSV $size "${pythonDockerCommand}" "python awk.py gawk" $queryType False $resultFile
#        queryTSV $size "${pythonDockerCommand}" "python awk.py nawk" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript base.R" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript readr.R 1_thread,not_lazy" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript readr.R 8_threads,notlazy" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript readr.R 8_threads,lazy" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript vroom.R 1_thread,no_altrep" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript vroom.R 8_threads,no_altrep" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript vroom.R 1_thread,altrep" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript vroom.R 8_threads,altrep" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript fread.R 1_thread" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript fread.R 8_threads" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript ff.R" $queryType False $resultFile
#        queryTSV $size "${rDockerCommand}" "Rscript arrow_csv.R" $queryType False $resultFile
#        queryTSV $size "${pythonDockerCommand}" "python pandas_csv.py c_engine,standard_io" $queryType False $resultFile
#        queryTSV $size "${pythonDockerCommand}" "python pandas_csv.py c_engine,memory_map" $queryType False $resultFile
#        queryTSV $size "${pythonDockerCommand}" "python pandas_csv.py python_engine,standard_io" $queryType False $resultFile
#        queryTSV $size "${pythonDockerCommand}" "python pandas_csv.py python_engine,memory_map" $queryType False $resultFile
#        queryTSV $size "${pythonDockerCommand}" "python pandas_csv.py pyarrow_engine,standard_io" $queryType False $resultFile
        # INFO: pyarrow does not support the 'memory_map' option.

# TODO: Update to python 3.11 when the pip installs will work properly.
#    done

    # An error is thrown when processing wide files in some cases, so we only test on small and tall files.
    #for size in "$small" "$tall"
#    for size in "$small"
#    do
#        queryTSV $size "${pythonDockerCommand}" "python DuckDB.py" $queryType False $resultFile
#    done
#done
#echo $resultFile
#cat $resultFile

#######################################################
# Create files in other formats.
# Limited to formats that write to a single file.
#######################################################

for f in data/*.tsv
#for f in data/10_90_1000.tsv
#for f in data/100000_900000_1000.tsv
do
#  $rDockerCommand Rscript convert_to_feather.R $f ${f/\.tsv/.fthr} &
#  $rDockerCommand Rscript convert_to_fst.R $f ${f/\.tsv/.fst} &
  $rDockerCommand Rscript convert_to_feather2.R $f ${f/\.tsv/.arw} &
  $rDockerCommand Rscript convert_to_parquet.R $f ${f/\.tsv/.prq} &
#  wait
#TODO: Record time, CPU, memory, and disk space
done

#TODO: Other formats:
#  hdf5?

# TODO: Repeat the TSV benchmarks when the files are gzipped?

# Prior benchmarks
  #https://pythonspeed.com/articles/pandas-read-csv-fast/ (shows examples of pyarrow and pyparquet)
  #https://www.danielecook.com/speeding-up-reading-and-writing-in-r/
  #https://cran.r-project.org/web/packages/vroom/vignettes/benchmarks.html
  #https://data.nozav.org/post/2019-r-data-frame-benchmark/ (multiple formats)

############################################################
# Measure how quickly we can query the files that have
# been compressed line-by-line.
############################################################

exit

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

  ### I am getting a segmentation fault sometimes.
  ##rm -f $outFile
  ##echo -e "$compressionMethod\t$compressionLevel\tC++\t$numDiscrete\t$numContinuous\t$numRows\t$( { /usr/bin/time -f %e ./TestFixedWidth4T $dataFile $transposedFile $colNamesFile $outFile $numDiscrete,$numDataPoints > /dev/null; } 2>&1 )" >> $resultFile
  ##./TestFixedWidth4T $dataFile $transposedFile $colNamesFile $outFile $numDiscrete,$numDataPoints
  ##python3 CheckOutput.py $outFile $masterOutFile
  
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

#NOTES:
#  When parsing tall file (Python):
#    About half of the time is used when calling parse_data_coords().
#    About 35% of the time is used to decompress the relevant lines in the main file.
#    About 10% of the time is used to write the output file.
#  When parsing wide file (Python):
#    Nearly all of the time is used to decompress the relevant lines in the main file.

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

if [ ! -f $resultFile ]
then
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
fi

############################################################
# Clean up the test files created so far to save disk space.
############################################################

#TODO:
#rm -rfv TestData/*
