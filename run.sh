#! /bin/bash

# Prior benchmarks
  #https://pythonspeed.com/articles/pandas-read-csv-fast/ (shows examples of pyarrow and pyparquet)
  #https://www.danielecook.com/speeding-up-reading-and-writing-in-r/
  #https://cran.r-project.org/web/packages/vroom/vignettes/benchmarks.html
  #https://data.nozav.org/post/2019-r-data-frame-benchmark/ (multiple formats)

# Interpreting output of time command:
#   https://stackoverflow.com/questions/556405/what-do-real-user-and-sys-mean-in-the-output-of-time1

#set -o errexit

#######################################################
# Set up Docker
#######################################################

pythonImage=tab_bench_python
rImage=tab_bench_r
rustImage=tab_bench_rust

#for dockerFile in Dockerfiles/tab_bench_*
#do
#    docker build -t $(basename $dockerFile) -f $dockerFile .
#done

baseDockerCommand="docker run -i -t --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox"
#baseDockerCommand="docker run -d --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox"
pythonDockerCommand="$baseDockerCommand $pythonImage"
rDockerCommand="$baseDockerCommand $rImage"
rustDockerCommand="$baseDockerCommand $rustImage"

#######################################################
# Create TSV files
#######################################################

mkdir -p data

small="10 90 1000"
tall="100 900 1000000"
wide="100000 900000 1000"

## Small file
#$pythonDockerCommand python /sandbox/BuildTsvFile.py $small /data/${small// /_}.tsv
## Tall, narrow file
#$pythonDockerCommand python /sandbox/BuildTsvFile.py $tall /data/${tall// /_}.tsv
## Short, wide file
#$pythonDockerCommand python /sandbox/BuildTsvFile.py $wide /data/${wide// /_}.tsv

#######################################################
# Convert files to other formats.
#######################################################

function convertTSV {
  numDiscrete=$1
  numNumeric=$2
  numRows=$3
  dockerCommand="$4"
  commandPrefix="$5"
  outExtension=$6
  resultFile=$7

  dataFile=data/${numDiscrete}_${numNumeric}_$numRows.tsv
  outFile=data/${numDiscrete}_${numNumeric}_${numRows}.${outExtension}

  rm -f $outFile

  echo -n -e "${outExtension}\t$numDiscrete\t$numNumeric\t$numRows\t" >> $resultFile
  
  command="${commandPrefix} $dataFile $outFile"

  $dockerCommand $command
#  $dockerCommand /usr/bin/time --verbose $command &> /tmp/result
#  $pythonDockerCommand python ParseTimeMemoryInfo.py /tmp/result >> $resultFile
#  echo >> $resultFile
}

conversionsResultFile=results/conversions.tsv

#echo -e "Extension\tNumDiscrete\tNumNumeric\tNumRows\tWallClockSeconds\tUserSeconds\tSystemSeconds\tMaxMemoryUsed_kilobytes" > $conversionsResultFile

#for size in "$small" "$tall" "$wide"
#for size in "$small"
#for size in "$tall"
#for size in "$wide"
#do
#  convertTSV $size "${rDockerCommand}" "Rscript convert_to_fst.R" fst $conversionsResultFile
#  convertTSV $size "${rDockerCommand}" "Rscript convert_to_feather.R" fthr $conversionsResultFile
#  convertTSV $size "${rDockerCommand}" "Rscript convert_to_arrow.R" arw $conversionsResultFile
#  convertTSV $size "${rDockerCommand}" "Rscript convert_to_parquet.R" prq $conversionsResultFile
#  convertTSV $size "${pythonDockerCommand}" "python convert_to_hdf5.py" hdf5 $conversionsResultFile
#  convertTSV $size "${pythonDockerCommand}" "python convert_to_fwf2.py" fwf2 $conversionsResultFile
#done

#NOTE: hdf5 fails when trying to write *wide* files in "table" mode. We can only read specific columns (rather than the whole) file in table mode, not fixed mode.
#for size in "$small" "$tall"
#do
#  convertTSV $size "${pythonDockerCommand}" "python convert_to_hdf5.py" hdf5 $conversionsResultFile
#done

#######################################################
# Query files. Filter based on values in 2 columns.
#   Then select other columns.
#######################################################

function queryFile {
  iteration=$1
  numDiscrete=$2
  numNumeric=$3
  numRows=$4
  dockerCommand="$5"
  commandPrefix="$6"
  queryType=$7
  columns=$8
  isMaster=$9
  inFileExtension=${10}
  resultFile=${11}

  dataFile=data/${numDiscrete}_${numNumeric}_${numRows}.${inFileExtension}
  outFile=/tmp/benchmark_files/${numDiscrete}_${numNumeric}_${numRows}_${queryType}_${columns}

  rm -f $outFile

  echo -n -e "${iteration}\t${commandPrefix}\t$queryType\t$columns\t$numDiscrete\t$numNumeric\t$numRows\t" >> $resultFile

  colNamesToKeep="all_columns"
  if [[ "$columns" == "firstlast_columns" ]]
  then
      colNamesToKeep="Discrete1,Discrete${numDiscrete},Numeric1,Numeric${numNumeric}"
  fi
  
  command="${commandPrefix} $queryType $dataFile $outFile Discrete2 Numeric2 $colNamesToKeep"

  echo Running query for ${iteration}, ${numDiscrete}, ${numNumeric}, ${numRows}, ${commandPrefix}, ${queryType}, ${columns}

#  $dockerCommand $command
  $dockerCommand /usr/bin/time --verbose $command &> /tmp/result
  $pythonDockerCommand python ParseTimeMemoryInfo.py /tmp/result >> $resultFile
  $pythonDockerCommand python ParseFileSize.py $outFile >> $resultFile
  echo >> $resultFile

  masterFile=/tmp/benchmark_files/${numDiscrete}_${numNumeric}_${numRows}_${queryType}_${columns}_master

  if [[ "$isMaster" == "False" ]]
  then
      if [ -f $outFile ]
      then
          echo Checking output for ${iteration}, ${numDiscrete}, ${numNumeric}, ${numRows}, ${commandPrefix}, ${queryType}, ${columns}
          python CheckOutput.py $outFile $masterFile
      else
          echo No output for ${iteration}, ${numDiscrete}, ${numNumeric}, ${numRows}, ${commandPrefix}, ${queryType}, ${columns}
      fi
  else
    echo Saving master file for ${iteration}, ${numDiscrete}, ${numNumeric}, ${numRows}, ${commandPrefix}, ${queryType}, ${columns}
    mv $outFile $masterFile
    echo "  Done"
  fi
}

#rm -rf /tmp/benchmark_files
mkdir -p /tmp/benchmark_files

mkdir -p results
resultFile=results/queries_uncompressed.tsv

echo -e "Iteration\tCommandPrefix\tQueryType\tColumns\tNumDiscrete\tNumNumeric\tNumRows\tWallClockSeconds\tUserSeconds\tSystemSeconds\tMaxMemoryUsed_kb\tOutputFileSize_kb" > $resultFile

#for iteration in 1..5
for iteration in {1..2}
do
    #for queryType in simple startsendswith
    for queryType in simple
    #for queryType in startsendswith
    do
#        for size in "$small" "$tall" "$wide"
        for size in "$small"
#        for size in "$tall"
#        for size in "$wide"
        do
#            for columns in firstlast_columns all_columns
            for columns in firstlast_columns
#            for columns in all_columns
            do
                isMaster=False
                if [[ "$iteration" == "1" ]]
                then
                    isMaster=True
                fi

                queryFile $iteration $size "${pythonDockerCommand}" "python line_by_line.py standard_io" $queryType $columns $isMaster tsv $resultFile

#                queryFile $iteration $size "${pythonDockerCommand}" "python line_by_line.py memory_map" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python awk.py awk" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python awk.py gawk" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python awk.py nawk" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript base.R" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript readr.R 1_thread,not_lazy" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript readr.R 8_threads,notlazy" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript readr.R 8_threads,lazy" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript vroom.R 1_thread,no_altrep" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript vroom.R 8_threads,no_altrep" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript vroom.R 1_thread,altrep" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript vroom.R 8_threads,altrep" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript fread.R 1_thread" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript fread.R 8_threads" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript ff.R" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript arrow_csv.R" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_csv.py c_engine,standard_io" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_csv.py c_engine,memory_map" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_csv.py python_engine,standard_io" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_csv.py python_engine,memory_map" $queryType $columns False tsv $resultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_csv.py pyarrow_engine,standard_io" $queryType $columns False tsv $resultFile
#                # INFO: pyarrow does not support the 'memory_map' option.
#                queryFile $iteration $size "${pythonDockerCommand}" "python duck_db.py" $queryType $columns False tsv $resultFile

#                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_hdf5.py" $queryType $columns False hdf5 $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript fst.R" $queryType $columns False fst $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript feather.R" $queryType $columns False fthr $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript arrow.R feather2" $queryType $columns False arw $resultFile
#                queryFile $iteration $size "${rDockerCommand}" "Rscript arrow.R parquet" $queryType $columns False prq $resultFile
                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2.py" $queryType $columns False fwf2 $resultFile
                queryFile $iteration $size "${rustDockerCommand}" "/Rust/fwf2/target/release/main" $queryType $columns False fwf2 $resultFile
# TODO: Update to python 3.11 when the pip installs will work properly.
            done
        done
    done
done

echo $resultFile
cat $resultFile



# TODO: Repeat the benchmarks when the files are compressed.
exit




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
