///////////////////////////////////
//                              //
//      Piccolo Lab             //
//      Test Fixed Width 3      //
//      James Wengler           //
//                              //
//////////////////////////////////

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
#include <string>
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

//This function takes an index and a mmap file, then returns the integar (as an int) found at that position
//Used to build the lineIndex array
int getIntFromCCFile(int coorFileMaxLength, char * coorFile, int indexToStart)
{
    char substring[coorFileMaxLength];
    memmove(substring, &coorFile[indexToStart], coorFileMaxLength);
    int position = atoi(substring);
    
    return position;
}

//This function accepts a char* filePath then returns a char* mmap file
//Used to open the data and .cc file
char* openMmapFile(char* filePath)
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
        //FIXME endpos is 0 when it shouldn't be (returns negative width which crashes later
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
            //cout << strToAdd << endl;
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

int main(int argc, char** argv)
{
    //All argv arguements
    string pathToLlFile = argv[1];
    char* dataPath = argv[2];
    char* pathToColFile = argv[3];
    char* outFilePath = argv[4];
    string pathToMCCL = argv[5];
    string colNamesFilePath = argv[6];
    string intNumRows = argv[7];
    char* pathToCtFile = argv[8];
    string queryColIndicesStr = argv[9];
    
    //Opens the line length file, pulls out an integer, and assigns it to lineLength
    int lineLength = readScalarFromFile(pathToLlFile);
    
    //Opens a memory mapped file to the .fwf2 data file
    char *dataMapFile = openMmapFile(dataPath);
    
    //Uses istringstream to pull an int from the command line
    int long long numRows = readScalarFromArgv(intNumRows);
    
    //Opens a memory mapped file to the .cc file
    char *ccMapFile = openMmapFile(pathToColFile);
    
    //Uses an ifstream to pull out an int for the maximum column coordinate length (max number of characters per line)
    int maxColumnCoordLength = readScalarFromFile(pathToMCCL);
    
    //Uses an ifstream to pull out each index for the column to be grabbed
    vector<int> lineIndex = createLineIndex(colNamesFilePath);
    unsigned long int lineIndexSize = lineIndex.size();
    int* lineIndexPointerArray = &lineIndex[0];
    
    //Create 2 arrays to be used in ParseDataCoordinates
    long long int colCoords[lineIndexSize];
    long long int colWidths[lineIndexSize];
    
    //Calls ParseDataCoordinates that populates the above arways with the starting postitions and widths
    parseDataCoords(lineIndexSize, lineIndexPointerArray, ccMapFile, maxColumnCoordLength, colCoords, colWidths);
    
    //Create a mmap file from the ct file
    char* ctFile = openMmapFile(pathToCtFile);
    
    //Create a vector from queryColIndicesStr
    vector<int> queryColIndices = makeQueryColVector(queryColIndicesStr);
    long long int colCoordsQuery[queryColIndices.size()];
    long long int colWidthsQuery[queryColIndices.size()];
    unsigned long int queryColSize = queryColIndices.size();
    int*queryColPointerArray = &queryColIndices[0];
    
    parseDataCoords(queryColSize, queryColPointerArray, ccMapFile, maxColumnCoordLength, colCoordsQuery, colWidthsQuery);
    
    vector<unsigned long int> matchingRows = filterRows(queryColIndices, numRows, colCoordsQuery, lineLength, colWidthsQuery, dataMapFile, ctFile);
    
    
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
    
    
    
    for (unsigned long int i = 0; i < matchingRows.size(); i++)
    {
        for (int j = 0; j < lineIndexSize - 1; j++)
        {

            long int coorToGrab = (colCoords[j] + (matchingRows[i] * lineLength));
            long long int width = colWidths[j];
            string strToAdd = "";
            createTrimmedValue(dataMapFile, coorToGrab, width, strToAdd);
            strToAdd += '\t';
            chunk += strToAdd;
            
        }
        long int coorToGrab = (colCoords[lineIndexSize - 1] + (matchingRows[i] * lineLength));
        long long int width = colWidths[lineIndexSize - 1];
        string strToAdd = "";
        createTrimmedValue(dataMapFile, coorToGrab, width, strToAdd);
        strToAdd += '\n';
        chunk += strToAdd;
        
        //Checks if the current chunk is still less in size than CHUNK_SIZE (a global variable)
        //if not, the chunk is written to the file
        if (chunkCount < CHUNK_SIZE)
        {
            chunkCount++;
        }
        
        else
        {
            //the .c_str() function converts chunk from char[] to char*[]
            fprintf(outFile, "%s", chunk.c_str());
            chunk = "";
            chunkCount = 0;
        }
        
    }
        
    
    //After the for loop, adds the remaing chunk to the file
    if (chunk.size() > 0)
    {
        fprintf(outFile, "%s", chunk.c_str());
    }
    
    return 0;
}

