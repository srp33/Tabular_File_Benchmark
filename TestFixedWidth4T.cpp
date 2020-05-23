/*
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */


#include <stdio.h>     // fprintf
#include <iostream>
#include <stdlib.h>    // free
#include </usr/include/zlib.h>
#include </usr/local/include/zstd.h>      // presumes zstd library is installed
#include "zstd/examples/common.h"    // Helper functions, CHECK(), and CHECK_ZSTD()
#include <string>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fstream>
#include <vector>
#include <ctype.h>
#include <unordered_map>

using namespace std;

const int CHUNK_SIZE = 1000;
bool isNumber (char* ctFile, int curIndex)
{
    if (ctFile[curIndex * 2] == 'n')
    {
        return true;
    }
    else
    {
        return false;
    }
}



//This function passes in a string by reference and removes the whitespace to the right
//Used to format the output
static inline void trimRightWhitespace(std::string &s)
{
    if(s.size() != 0){
        unsigned long int endOfWhitespace = 0;
        unsigned long int stringSize = s.size();
        for (unsigned long int i = stringSize; i > 0; i--)
        {
            if (s[i] != ' ' && s[i] != '\0'  && s[i] != '\n')
            {
                endOfWhitespace = i;
                break;
            }

        }

        s.erase(endOfWhitespace + 1, s.size());
    }


}

//This function takes an index and a mmap file, then returns the integar (as an int) found at that position
//Used to build the lineIndex array
long long int getIntFromCCFile(int coorFileMaxLength, char * coorFile, long long int indexToStart)
{
    char substring[coorFileMaxLength];
    memmove(substring, &coorFile[indexToStart], coorFileMaxLength);
    long long int position = atoi(substring);

    return position;
}

//This function accepts a char* filePath then returns a char* mmap file
//Used to open the data and .cc file
char* openMmapFile(const char* filePath)
{
    int intFileObject = open(filePath, O_RDONLY, 0);
    if (intFileObject < 0)
    {
        cerr << "Unable to open " << filePath << " for input." << endl;
        exit(1);
    }

    //lseek finds the end of the file (to be used in opening a datamapped file)
    long long dataFileSize = lseek(intFileObject, 0, SEEK_END);
    char *dataFile = reinterpret_cast<char*>(mmap(NULL, dataFileSize, PROT_READ, MAP_FILE | MAP_SHARED, intFileObject, 0));

    return dataFile;
}

//This funtion reads a single integar from a file
//Used for ll and mccl file
int readScalarFromFile(string filePath)
{
    ifstream grabInt(filePath);
    int intToGet;
    grabInt >> intToGet;

    return intToGet;
}

//This function reads a single integar from agrv
//Used for numRows
int long long readScalarFromArgv(string arguement)
{
    istringstream getRows(arguement);
    int long long scalar;
    getRows >> scalar;

    return scalar;
}

//This function returns a vector of the columns that the user wants to project
//Used to make a vector from the _columns_tsv file
vector<int> createLineIndex(string filePath)
{
    string stringToReadFrom = "";
    vector<int> lineIndex = {};
    //"odd" is responsible for only pulling the integers out of the file by skipping all the strings (input file is int-string-int-string etc)
    int odd = 0;    ifstream columns(filePath);
    while (columns >> stringToReadFrom)
    {
        if (odd % 2 == 0)
        {
            //Converts the string (From the file) to an int (for use later)
            istringstream toInt(stringToReadFrom);
            int numericID;
            toInt >> numericID;
            lineIndex.push_back(numericID);
        }

        odd++;
    }

    return lineIndex;
}
//This function takes in a mmap file, a coordinate, the width of the column, and a string by reference
//The string is modified to contain whatever is at the specified coordinate with no trailing whitespace
//Used to format the output
void createTrimmedValue(char * mmapFile, long int coorToGrab, long long int width, string &myString)
{
    char substringFromFile[width];
    memmove(substringFromFile, &mmapFile[coorToGrab], width);
    substringFromFile[width] = '\0';
    myString.assign(substringFromFile);
    trimRightWhitespace(myString);
}

//This function passes in 2 arrays by reference, and then using the lineIndex array, populates them with the
//start position and width of each column the user wants to project
//Used to create the arrays containing the data for each column
void parseDataCoords(unsigned long int lineIndexSize, int* lineIndices, char * coordsFile, int coordsFileMaxLength, long long int* startPositions, long long int* widths)
{
    for (int i = 0; i < lineIndexSize; i++)
    {
        int column = lineIndices[i];
        int indexToStart = (column * (coordsFileMaxLength + 1));
        int startPos = getIntFromCCFile(coordsFileMaxLength, coordsFile, indexToStart);
        startPositions[i] = startPos;

        long long int endPos = getIntFromCCFile(coordsFileMaxLength, coordsFile, (indexToStart + coordsFileMaxLength + 1));
        long long int width = (endPos - startPos);
        widths[i] = width;


    }


}

vector<int> makeQueryColVector(string csvValues)
{
    vector<int> indices;
    size_t found = csvValues.find(",");
    if (found == string::npos)
    {
        cerr << "No comma found in columns to query, expected 'col,col' got '" << csvValues << "'";
        exit(1);
    }
    else
    {
        string firstCol = csvValues.substr(0, found);
        string secondCol = csvValues.substr(found + 1, csvValues.size() - 1);
        indices.push_back(atoi(firstCol.c_str()));
        indices.push_back(atoi(secondCol.c_str()));

    }

    return indices;
}

vector<unsigned long int> filterRows (vector<int> queryColIndices, int long long numRows, long long int* colCoords, int lineLength, long long int* colWidths, char* dataMapFile, char* ctFile)
{
    vector<unsigned long int> matchingRows;
    //By default the header row is included
    matchingRows.push_back(0);
    for (unsigned long int i = 1; i < numRows; i++)
    {
        int colsAdded = 0;
        for (int j = 0; j < queryColIndices.size(); j++)
        {
            long int coorToGrab = (colCoords[j] + (i * lineLength));
            long long int width = colWidths[j];
            string strToAdd = "";
            createTrimmedValue(dataMapFile, coorToGrab, width, strToAdd);
            int curIndex = queryColIndices[j];
            if (isNumber(ctFile, curIndex))
            {
                float tempInt = atof(strToAdd.c_str());
                if (tempInt >= .1)
                {
                    colsAdded++;
                }
                else
                {
                    break;
                }

            }
            else
            {
                if (strToAdd[0] == 'A' || strToAdd[strToAdd.size() - 1] == 'Z')
                {
                    colsAdded++;
                }
                else
                {
                    break;
                }

            }
        }
        if(colsAdded == queryColIndices.size())
        {
            matchingRows.push_back(i);
        }
    }
    return matchingRows;
}






static void decompressFile_orDie(const char* fname)
{
    FILE* const fin  = fopen_orDie(fname, "rb");
    size_t const buffInSize = ZSTD_DStreamInSize();
    void*  const buffIn  = malloc_orDie(buffInSize);
    FILE* const fout = stdout;
    size_t const buffOutSize = ZSTD_DStreamOutSize();
    void*  const buffOut = malloc_orDie(buffOutSize);

    ZSTD_DCtx* const dctx = ZSTD_createDCtx();
    CHECK(dctx != NULL, "ZSTD_createDCtx() failed!");

    /* This loop assumes that the input file is one or more concatenated zstd
     * streams. This example won't work if there is trailing non-zstd data at
     * the end, but streaming decompression in general handles this case.
     * ZSTD_decompressStream() returns 0 exactly when the frame is completed,
     * and doesn't consume input after the frame.
     */
    size_t const toRead = buffInSize;
    size_t read;
    size_t lastRet = 0;
    int isEmpty = 1;
    while ( (read = fread_orDie(buffIn, toRead, fin)) ) {

        isEmpty = 0;
        ZSTD_inBuffer input = { buffIn, read, 0 };
        /* Given a valid frame, zstd won't consume the last byte of the frame
         * until it has flushed all of the decompressed data of the frame.
         * Therefore, instead of checking if the return code is 0, we can
         * decompress just check if input.pos < input.size.
         */
//        while (input.pos < input.size) {
            ZSTD_outBuffer output = { buffOut, buffOutSize, 0};
            /* The return code is zero if the frame is complete, but there may
             * be multiple frames concatenated together. Zstd will automatically
             * reset the context when a frame is complete. Still, calling
             * ZSTD_DCtx_reset() can be useful to reset the context to a clean
             * state, for instance if the last decompression call returned an
             * error.
             */
            size_t const ret = ZSTD_decompressStream(dctx, &output , &input);
            CHECK_ZSTD(ret);
            fwrite_orDie(buffOut, output.pos, fout);
            lastRet = ret;
            break;

//        }
    }

    if (isEmpty) {
        fprintf(stderr, "input is empty\n");
        exit(1);
    }

    if (lastRet != 0) {
        /* The last return value from ZSTD_decompressStream did not end on a
         * frame, but we reached the end of the file! We assume this is an
         * error, and the input was truncated.
         */
        fprintf(stderr, "EOF before end of stream: %zu\n", lastRet);
        exit(1);
    }

    ZSTD_freeDCtx(dctx);
    fclose_orDie(fin);
    fclose_orDie(fout);
    free(buffIn);
    free(buffOut);
}

static void decompressOneLine(const char* fname, size_t pos, void* &buffOut, size_t const buffOutSize, size_t ll)
{
    FILE* const fin  = fopen(fname, "r+");
    size_t const buffInSize = ZSTD_DStreamInSize();
    void*  const buffIn  = malloc_orDie(10000000000);
    ZSTD_DCtx* const dctx = ZSTD_createDCtx();
    CHECK(dctx != NULL, "ZSTD_createDCtx() failed!");

    size_t read;
    int isEmpty = 1;
    fseek(fin, 0, SEEK_SET);
    fseek(fin, pos, SEEK_CUR);
    //10000 = ll - 1
    while ( (read = fread(buffIn, 1, ll, fin)) ) {

        isEmpty = 0;
        ZSTD_inBuffer input = { buffIn, read, 0 };

        ZSTD_outBuffer output = { buffOut, 10000000000, 0};

        size_t const ret = ZSTD_decompressStream(dctx, &output , &input);
        free(buffIn);
        ZSTD_freeDCtx(dctx);
        fclose_orDie(fin);

        cout << "Ret value : " << ret << endl;


        break;
    }

}

vector<unsigned long int> findMatchingRows(char* outBuffer, long long int mccl, int ll, char* ctFile, int rowNumber){
    vector <unsigned long int> temp;

    unsigned long int rowIndex = 1;

    bool num = isNumber(ctFile, rowNumber);
    for(int i = 0; i < (ll - 1); i += mccl){
        string value;

        createTrimmedValue(outBuffer, i, mccl, value);
        if(value.size() != 0){
            if(num){

                float tempInt = atof(value.c_str());
                if (tempInt > .1){
                    temp.push_back(rowIndex);
                }

            }else{

                if (value[0] == 'A' || value[value.size() - 1] == 'Z'){
                    temp.push_back(rowIndex);
                }
            }
        }

        rowIndex++;
    }
    free(outBuffer);

    return temp;
}

vector<unsigned long int> filterRowsTransposed (const char * transposedFile, vector<int> queryColIndices, const char * dataFilePath, long long int* widths, int lineLength, char* ctFile){

    string pathToMccl(transposedFile);
    pathToMccl += ".mrsl";

    string rowStart(transposedFile);
    rowStart += ".rowstart";

    const char* rowStartCharStar = rowStart.c_str();

    char* rowStartFile = openMmapFile(rowStartCharStar);

    int mccl = readScalarFromFile(pathToMccl);

    vector<unsigned long int> matchingRows;
    vector<unsigned long int> col1;
    vector<unsigned long int> col2;
    matchingRows.push_back(0);

    /* Decompress */

    for(int i = 0; i < queryColIndices.size(); i++){

        int pos = (mccl + 1) * queryColIndices.at(i);
        size_t index = getIntFromCCFile(mccl, rowStartFile, pos);
        size_t const buffOutSize = ZSTD_DStreamOutSize();
        void*  buffOut = malloc_orDie(10000000000);
        decompressOneLine(transposedFile, index, buffOut, buffOutSize, lineLength);
        long long int curColWidth = widths[i];
        if (i == 0){
            col1 = findMatchingRows((char*)buffOut, curColWidth, lineLength, ctFile, queryColIndices.at(i));
            //cout << "Buffer : " << (char*)buffOut << endl;
        }
        else{
            col2 = findMatchingRows((char*)buffOut, curColWidth, lineLength, ctFile, queryColIndices.at(i));
            //cout << "Buffer : " << (char*)buffOut << endl;
        }
    }

    for (int i = 0; i < col1.size(); i++){
        for (int j = 0; j < col2.size(); j++){
            if (col1.at(i) == col2.at(j)){
                matchingRows.push_back(col2.at(j));

            }
        }
    }

//    for (int i = 0; i < matchingRows.size(); i++){
//        cout << "Row : " << matchingRows.at(i) << endl;
//    }
    return matchingRows;


}


int main(int argc, const char** argv)
{

//    //OLD ARGS
//    const char* const exeName = argv[0];
//    const char*  transposedPath = argv[1]; ///Users/jameswengler/TFB/Transposed/10_90_1000.fwf2.zstd_1
//    const char*  dataPath = argv[2];  //../TestData/10_90_1000.fwf2
//    const char*  colNamesFilePath = argv[3]; //../TestData/10_90_1000.fwf2_columns.tsv
//    string queryColIndicesStr = argv[4]; // 10,100
//    const char* outFilePath = argv[5]; //../TestData/Output.txt

    //NEW ARGS
    const char*  dataPath = argv[1]; //~/TempDir/Temp/10_90_1000.fwf2
    const char*  transposedPath = argv[2]; //~/TempDir/Temp/10_90_1000.fwf2.zstd_1
    const char*  colNamesFilePath = argv[3]; //~/TempDir/Temp/10_90_1000_columns.tsv
    const char* outFilePath = argv[4]; //~/TempDir/Temp/Output.txt
    string queryColIndicesStr = argv[5]; //10,100

    string pathToLlFile(dataPath);
    pathToLlFile += ".ll";

    string ctFilePath(dataPath);
    ctFilePath += ".ct";

    char* ctFile = openMmapFile((char*)ctFilePath.c_str());

    string colFile(dataPath);
    colFile += ".cc";
    const char* pathToColFile = colFile.c_str();

    string pathToMCCL(dataPath);
    pathToMCCL += ".mccl";

    string transposedCC(transposedPath);
    transposedCC += ".cc";

    string transposedData(transposedPath);
    //transposedData += ".zstd_1";

    string transposedMCCL(transposedPath);
    transposedMCCL += ".mccl";

    string pathToLlFileTransposed(transposedPath);
    pathToLlFileTransposed += ".ll";

    //Opens the line length file, pulls out an integer, and assigns it to lineLength
    int lineLength = readScalarFromFile(pathToLlFile);
    //cout << lineLength << endl;

    int lineLengthTransposed = readScalarFromFile(pathToLlFileTransposed);

    //Opens a memory mapped file to the .fwf2 data file
    char *dataMapFile = openMmapFile(dataPath);

    //Opens a memory mapped file to the .cc file
    char *ccMapFile = openMmapFile(pathToColFile);

    char*ccMapFileTransposed = openMmapFile((char*)transposedCC.c_str());

    //Uses an ifstream to pull out an int for the maximum column coordinate length (max number of characters per line)
    int maxColumnCoordLength = readScalarFromFile(pathToMCCL);

    int maxColumnCoordLengthTranspoed = readScalarFromFile(transposedMCCL);

    //Uses an ifstream to pull out each index for the column to be grabbed
    vector<int> lineIndex = createLineIndex(colNamesFilePath);
    unsigned long int lineIndexSize = lineIndex.size();
    int* lineIndexPointerArray = &lineIndex[0];

    //Create 2 arrays to be used in ParseDataCoordinates
    long long int colCoords[lineIndexSize];
    long long int colWidths[lineIndexSize];

    //Calls ParseDataCoordinates that populates the above arrays with the starting postitions and widths
    parseDataCoords(lineIndexSize, lineIndexPointerArray, ccMapFile, maxColumnCoordLength, colCoords, colWidths);

    //Create a vector from queryColIndicesStr
    vector<int> queryColIndices = makeQueryColVector(queryColIndicesStr);
    long long int colCoordsQuery[queryColIndices.size()];
    long long int colWidthsQuery[queryColIndices.size()];
    unsigned long int queryColSize = queryColIndices.size();
    int*queryColPointerArray = &queryColIndices[0];

    parseDataCoords(queryColSize, queryColPointerArray, ccMapFileTransposed, maxColumnCoordLengthTranspoed, colCoordsQuery, colWidthsQuery);

    //Found the problem, for the second parseDataCoords I need to pass in the files found in the /Transposed directory, and not the files found in /TestData. That is, except for actual data file. That file needs to be the file found in /TestData
    //Solution: Pass in two file paths. One for the normal data (/TestData) and another for the transposed files (/Transposed)
    //ERROR IS IN THE LINE BELOW
    vector<unsigned long int> matchingRows = filterRowsTransposed((char*)transposedData.c_str(), queryColIndices, dataPath, colWidthsQuery, lineLengthTransposed, ctFile);
    //ERROR IS IN THE LINE ABOVE

    //Uses a FILE object to open argv[4] as an output file
    //Implements chunking to reduce writing calls to the file
    //Writes to the file using fprintf (C syntax, and notably faster than C++)
    //"chunk" is the string that is built to be written to the file
    string chunk = "";
    int chunkCount = 0;
    FILE* outFile =  fopen(outFilePath, "w");
    if (outFile == NULL)
    {
        cerr << "Failed to open output file (was NULL)" << endl;
        exit(1);
    }


    string pathToMccl(dataPath);
    //pathToMccl += ".mrsl";
    pathToMccl += ".mccl";

    string rowStart(dataPath);
    rowStart += ".rowstart";

    const char* rowStartCharStar = rowStart.c_str();

    char* rowStartFile = openMmapFile(rowStartCharStar);

    int mccl = readScalarFromFile(pathToMccl);

    ofstream outfile ("~/testOutput.txt",std::ofstream::binary);

    string ccNonTransposed(dataPath);
    ccNonTransposed += ".cc";

    char* ccNonTransposedChar = openMmapFile(ccNonTransposed.c_str());

    string pathToMcclNonTrans(dataPath);
    pathToMcclNonTrans += ".mccl";
    int newMccl = readScalarFromFile(pathToMcclNonTrans);
    string pathToMrsl(dataPath);
    pathToMrsl += ".mrsl";
    int mrsl = readScalarFromFile(pathToMrsl);

    for (unsigned long int i = 0; i < matchingRows.size(); i++)
    {
        //cout << "Row Number : " << matchingRows.at(i) << endl;

        unsigned long long int pos = (mrsl + 1) * matchingRows.at(i);
        //cout << "Pos : " << pos << endl;
        unsigned long long int index = getIntFromCCFile(mrsl, rowStartFile, pos);
        //cout << "Index : " << index << endl;
        size_t const buffOutSize = ZSTD_DStreamOutSize();
        void* buffOut = malloc_orDie(10000000000);

        //FIXME
        //Only decompressing when index = 0;
        decompressOneLine(dataPath, index, buffOut, buffOutSize, lineLength);
        //cout << endl;

//        if(i == 0){
//            cout << "Column : " << matchingRows.at(i) << endl;
//            cout << "Pos : " << pos << endl;
//            cout << "MRSL : " << mrsl << endl;
//            cout << "Index : " << index << endl;
//            cout << "Rowstart Path : " << rowStart << endl;
//            cout << "Buffer for row " << matchingRows.at(10) << " : " << (char*)buffOut << endl << endl;
//            break;
//        }
        for(int j = 0; j < lineIndex.size(); j++){

            long int coorToGrab = colCoords[j];
            long int width = colWidths[j];
            string value;

            createTrimmedValue((char*)buffOut, coorToGrab, width, value);
            chunk += value;
            if(j != lineIndex.size() - 1){
                chunk += "\t";
            }




        }
        //cout << "Made it here!" << endl;
        //free(buffOut);
        chunk += "\n";
        if (chunkCount < CHUNK_SIZE)
        {
            chunkCount++;
        }
        else
        {
            //the .c_str() function converts chunk from char[] to char*[]
            fprintf(outFile, "%s", chunk.c_str());
            //cout << "Printed a value" << endl;
            chunk = "";
            chunkCount = 0;
        }




    }




//    After the for loop, adds the remaing chunk to the file
    if (chunk.size() > 0)
    {
        fprintf(outFile, "%s", chunk.c_str());
    }

    return 0;
}



