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

currentDir="$(pwd)"
tmpDir=/tmp/build_docker

function buildDockerImage {
    dockerFileName=$1
    otherDir="$2"

    rm -rf $tmpDir
    mkdir -p $tmpDir

    dockerFilePath=Dockerfiles/$dockerFileName

    cp $dockerFilePath $tmpDir/

    if [[ "$otherDir" != "" ]]
    then
        cp -r "$otherDir"/* $tmpDir/
    fi

    cd $tmpDir
    docker build -t $dockerFileName -f $dockerFileName .
    cd $currentDir
}

buildDockerImage tab_bench_python
#buildDockerImage tab_bench_r
#buildDockerImage tab_bench_rust $currentDir/Rust

#baseDockerCommand="docker run -i -t --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox"
baseDockerCommand="docker run -i --rm --user $(id -u):$(id -g) -v $(pwd):/sandbox -v $(pwd)/data:/data -v /tmp:/tmp --workdir=/sandbox"
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

queryResultFile=results/queries_uncompressed.tsv

echo -e "Iteration\tCommandPrefix\tQueryType\tColumns\tNumDiscrete\tNumNumeric\tNumRows\tWallClockSeconds\tUserSeconds\tSystemSeconds\tMaxMemoryUsed_kb\tOutputFileSize_kb" > $queryResultFile

#for iteration in {1..5}
#for iteration in {1..1}
#do
#    #for queryType in simple startsendswith
#    for queryType in simple
#    #for queryType in startsendswith
#    do
##        for size in "$small" "$tall" "$wide"
#        for size in "$small"
#        for size in "$tall"
##        for size in "$wide"
#        do
#            for columns in firstlast_columns all_columns
#            for columns in firstlast_columns
##            for columns in all_columns
#            do
#                isMaster=False
#                if [[ "$iteration" == "1" ]]
#                then
#                    isMaster=True
#                fi
#
#                queryFile $iteration $size "${pythonDockerCommand}" "python line_by_line.py standard_io" $queryType $columns $isMaster tsv $queryResultFile
#
##                queryFile $iteration $size "${pythonDockerCommand}" "python line_by_line.py memory_map" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${pythonDockerCommand}" "python awk.py awk" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${pythonDockerCommand}" "python awk.py gawk" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${pythonDockerCommand}" "python awk.py nawk" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript base.R" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript readr.R 1_thread,not_lazy" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript readr.R 8_threads,notlazy" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript readr.R 8_threads,lazy" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript vroom.R 1_thread,no_altrep" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript vroom.R 8_threads,no_altrep" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript vroom.R 1_thread,altrep" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript vroom.R 8_threads,altrep" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript fread.R 1_thread" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript fread.R 8_threads" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript ff.R" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript arrow_csv.R" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_csv.py c_engine,standard_io" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_csv.py c_engine,memory_map" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_csv.py python_engine,standard_io" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_csv.py python_engine,memory_map" $queryType $columns False tsv $queryResultFile
##                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_csv.py pyarrow_engine,standard_io" $queryType $columns False tsv $queryResultFile
##                # INFO: pyarrow does not support the 'memory_map' option.
##                queryFile $iteration $size "${pythonDockerCommand}" "python duck_db.py" $queryType $columns False tsv $queryResultFile
#
##                queryFile $iteration $size "${pythonDockerCommand}" "python pandas_hdf5.py" $queryType $columns False hdf5 $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript fst.R" $queryType $columns False fst $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript feather.R" $queryType $columns False fthr $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript arrow.R feather2" $queryType $columns False arw $queryResultFile
##                queryFile $iteration $size "${rDockerCommand}" "Rscript arrow.R parquet" $queryType $columns False prq $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2.py" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${rustDockerCommand}" "/Rust/fwf2/target/release/main" $queryType $columns False fwf2 $queryResultFile
## TODO: Update to python 3.11 when the pip installs will work properly.
#            done
#        done
#    done
#done

############################################################
# Build compressed versions of the fixed-width files using
# a variety of compression algorithms. Each line in the data
# is compressed individually.
############################################################

function compressLines {
  resultFile=$1
  numDiscrete=$2
  numNumeric=$3
  numRows=$4
  method=$5
  level=$6

  inFile=data/${numDiscrete}_${numNumeric}_${numRows}.fwf2
  outFile=data/compressed/${numDiscrete}_${numNumeric}_${numRows}.fwf2.${method}

  if [[ "$level" != "NA" ]]
  then
    outFile=${outFile}_${level}
  fi

  command="python3 compress_lines.py $inFile $numRows $method $level $outFile"
  echo Running "$command"

  echo -n -e "${numDiscrete}\t${numNumeric}\t${numRows}\t${method}\t${level}\t" >> $resultFile

#  $pythonDockerCommand $command
  $pythonDockerCommand /usr/bin/time --verbose $command &> /tmp/result
  $pythonDockerCommand python ParseTimeMemoryInfo.py /tmp/result >> $resultFile
  $pythonDockerCommand python ParseFileSize.py ${outFile}* >> $resultFile
  echo >> $resultFile
}

function compressLinesAll {
    resultFile=$1
    size="$2"

    compressLines $resultFile $size bz2 1
    compressLines $resultFile $size bz2 5
    compressLines $resultFile $size bz2 9
    compressLines $resultFile $size gz 1
    compressLines $resultFile $size gz 5
    compressLines $resultFile $size gz 9
    compressLines $resultFile $size lzma NA
    compressLines $resultFile $size snappy NA
    compressLines $resultFile $size zstd 1
    compressLines $resultFile $size zstd 5
    compressLines $resultFile $size zstd 9
    compressLines $resultFile $size zstd 22
    compressLines $resultFile $size lz4 0
    compressLines $resultFile $size lz4 5
    compressLines $resultFile $size lz4 10
    compressLines $resultFile $size lz4 16
}

mkdir -p data/compressed

compressLinesResultFile=results/compress_lines.tsv

#echo -e "NumDiscrete\tNumNumeric\tNumRows\tMethod\tLevel\tWallClockSeconds\tUserSeconds\tSystemSeconds\tMaxMemoryUsed_kb\tOutputFileSize_kb" > $compressLinesResultFile

#compressLinesAll $compressLinesResultFile "$small"
#compressLinesAll $compressLinesResultFile "$tall"
#compressLinesAll $compressLinesResultFile "$wide"

#cat $compressLinesResultFile

############################################################
# Measure how quickly we can query the files that have
# been compressed line-by-line.
############################################################

##for iteration in {1..5}
#for iteration in {1..1}
#do
#    #for queryType in simple startsendswith
#    for queryType in simple
#    #for queryType in startsendswith
#    do
#        for size in "$small" "$tall" "$wide"
#        #for size in "$small"
#        #for size in "$tall"
#        #for size in "$wide"
#        do
#            #for columns in firstlast_columns all_columns
#            for columns in firstlast_columns
#            #for columns in all_columns
#            do
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py bz2 1" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py bz2 5" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py bz2 9" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py gz 1" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py gz 5" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py gz 9" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py lzma NA" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py snappy NA" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py zstd 1" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${rustDockerCommand}" "/Rust/fwf2_cmpr/target/release/main zstd 1" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py zstd 5" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${rustDockerCommand}" "/Rust/fwf2_cmpr/target/release/main zstd 5" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py zstd 9" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${rustDockerCommand}" "/Rust/fwf2_cmpr/target/release/main zstd 9" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py zstd 22" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${rustDockerCommand}" "/Rust/fwf2_cmpr/target/release/main zstd 22" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py lz4 0" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py lz4 5" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py lz4 10" $queryType $columns False fwf2 $queryResultFile
#                queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr.py lz4 16" $queryType $columns False fwf2 $queryResultFile
#            done
#        done
#    done
#done

#echo $queryResultFile
#cat $queryResultFile

############################################################
# Build compressed versions of the fixed-width files that
# have a transposed version of the data. Each line is 
# compressed individually.
############################################################

function transposeAndCompressLines {
  resultFile=$1
  numDiscrete=$2
  numNumeric=$3
  numRows=$4
  method=$5
  level=$6

  inFile1=data/${numDiscrete}_${numNumeric}_${numRows}.fwf2
  inFile2=data/compressed/${numDiscrete}_${numNumeric}_${numRows}.fwf2.${method}_${level}
  outFile1=data/transposed/${numDiscrete}_${numNumeric}_${numRows}.fwf2
  outFile2=data/transposed_and_compressed/${numDiscrete}_${numNumeric}_${numRows}.fwf2.${method}_${level}
  transposedNumRows=$((numDiscrete + numNumeric))

  if [ ! -f $outFile1 ]
  then
    echo Transpose $inFile1 to $outFile1
    $pythonDockerCommand python3 transpose_fwf2.py $inFile1 $outFile1
  fi

  echo Compressing $outFile1 to $outFile2
  $pythonDockerCommand python3 compress_lines.py $outFile1 $transposedNumRows $method $level $outFile2

  echo -n -e "${numDiscrete}\t${numNumeric}\t${numRows}\t${method}\t${level}" >> $resultFile
  $pythonDockerCommand python ParseFileSize.py ${inFile1}* >> $resultFile
  $pythonDockerCommand python ParseFileSize.py ${inFile2}* >> $resultFile
  $pythonDockerCommand python ParseFileSize.py ${outFile2}* >> $resultFile
  echo >> $resultFile
}

mkdir -p data/transposed data/transposed_and_compressed

tcResultFile=results/transposed_compressed.tsv

#echo -e "NumDiscrete\tNumNumeric\tNumRows\tMethod\tLevel\tUncompressedSize_kb\tPortraitCompressedSize_kb\tLandscapeCompressedSize_kb" > $tcResultFile

#for level in 1 5 9 22
#do
#    transposeAndCompressLines $tcResultFile $small zstd $level
#    transposeAndCompressLines $tcResultFile $tall zstd $level
#    transposeAndCompressLines $tcResultFile $wide zstd $level
#done

#cat $tcResultFile

############################################################
# Measure how quickly we can query the files that have
# been compressed line-by-line. This time use the transposed
# version of the data for filtering.
############################################################

queryResultFile=results/queries_compressed_transposed.tsv
echo -e "Iteration\tCommandPrefix\tQueryType\tColumns\tNumDiscrete\tNumNumeric\tNumRows\tWallClockSeconds\tUserSeconds\tSystemSeconds\tMaxMemoryUsed_kb\tOutputFileSize_kb" > $queryResultFile

#for iteration in {1..5}
#for iteration in {1..1}
#do
#    for queryType in simple startsendswith
#    #for queryType in simple
#    #for queryType in startsendswith
#    do
#        for size in "$small" "$tall" "$wide"
#        #for size in "$small"
#        #for size in "$tall"
#        #for size in "$wide"
#        do
#            for columns in firstlast_columns all_columns
#            #for columns in firstlast_columns
#            #for columns in all_columns
#            do
#                for level in 1 5 9 22
#                #for level in 1
#                do
#                    queryFile $iteration $size "${pythonDockerCommand}" "python fwf2_cmpr_trps.py zstd ${level}" $queryType $columns False fwf2 $queryResultFile
#                    queryFile $iteration $size "${rustDockerCommand}" "/Rust/fwf2_cmpr_trps/target/release/main zstd ${level}" $queryType $columns False fwf2 $queryResultFile
#                done
#            done
#        done
#    done
#done

#echo $queryResultFile
#cat $queryResultFile

############################################################
# Real-world data: ARCHS4
############################################################

mkdir -p data/archs4

#$pythonDockerCommand wget -O data/archs4/human_tpm_v11.h5 https://s3.amazonaws.com/mssm-seq-matrix/human_tpm_v11.h5
$pythonDockerCommand python convert_archs4_hdf5_to_tsv.py data/archs4/human_tpm_v11.h5 data/archs4/human_tpm_v11_sample.tsv.gz data/archs4/human_tpm_v11_expr.tsv.gz
#$pythonDockerCommand python convert_to_fwf2.py data/archs4/human_tpm_v11_sample.tsv.gz data/archs4/human_tpm_v11_sample.fwf2
#$pythonDockerCommand python convert_to_fwf2.py data/archs4/human_tpm_v11_expr.tsv.gz data/archs4/human_tpm_v11_expr.fwf2
exit

#TODO: Try two levels of compression with fst (and next fastest).
#      First, compress.
#      Second, query.
#TODO: Real-world biology data
#        Find a suitable file and replace genotype tests with that?
#        Make sure some columns have discrete values with varying lengths.
#        Convert it to fwf2, cmpr, trps.
#          See how well compression works.
#          - ARCHS4 Version 2 (Ensembl 107), TPM (transcript level, human, 101 GB, 11-16-2021)
#            https://maayanlab.cloud/archs4/download.html
#            Show that we can transpose such a large file.
#          - CADD files
#            https://krishna.gs.washington.edu/download/CADD/v1.6/GRCh38/
#            What query would be biologically relevant?
#            Show that it is fast here but that compression is poor. Address compression in F4 paper?
#            Go with the file that doesn't include the annotations?
#            Transposing doesn't really make sense for this one.
#TODO: Use F4 instead? Leaning away from it.
#      Build a simple web-server container (fastAPI?) to demonstrate remote queries.
#        Peek
#        Filter/query
#TODO: Clean up scripts.
#        Remove ct stuff when building fwf2.
#TODO: Run everything from beginning to end (sensei? miyagi?).

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

#TODO: Move this up before we do anything with compression?
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
#  runGenotypeTests $resultFile 50
#  runGenotypeTests $resultFile 100
#  runGenotypeTests $resultFile 500
#  runGenotypeTests $resultFile 1000
#  runGenotypeTests $resultFile 5000
#  runGenotypeTests $resultFile 10000
#  runGenotypeTests $resultFile 50000
#  runGenotypeTests $resultFile 100000
#  runGenotypeTests $resultFile 500000
#fi

############################################################
# Clean up the test files created so far to save disk space.
############################################################

#TODO:
#rm -rfv TestData/*
