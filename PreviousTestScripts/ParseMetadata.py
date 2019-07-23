import numpy as np
import sys, gzip
import time

instInfoFile = sys.argv[1]
cellInfo = sys.argv[2]
pertInfo = sys.argv[3]
pertMetrics = sys.argv[4]
metadataOut = sys.argv[5]

def readIntoDict(filePath, startIndex):
    infoDict = {}
    with gzip.open(filePath) as f:
        headerList = f.readline().decode().rstrip('\n').split('\t')

        for line in f:
            lineList = line.decode().rstrip('\n').split('\t')
            sample = lineList[0]

            infoDict[sample] = {}

            for i in range(1, len(lineList)):
                variable = headerList[i]
                value = checkMissing(lineList[i])

                infoDict[sample][variable] = value

    return infoDict

def checkMissing(value):
    if value in ("-666.0", "-666"):
        value = "NA"

    return value

cellInfoDict = readIntoDict(cellInfo, 1)
pertInfoDict = readIntoDict(pertInfo, 3)
pertMetricsDict = readIntoDict(pertMetrics, 3)

with gzip.open(instInfoFile, 'r') as instInfo:
    with gzip.open(metadataOut, 'w') as metaOut:
        headerList = instInfo.readline().decode().rstrip('\n').split('\t')

        metaOut.write("Sample\tVariable\tValue\n".encode())
        index = 0
        for row in instInfo:
            rowList = row.decode().strip('\n').split('\t')

            index = index + 1
            if index % 1000 == 0:
                print(str(index) + " of metadata")
                sys.stdout.flush()

            sample = rowList[0]
            sampleDict = {}

            for i in range(1, len(rowList)):
                variable = headerList[i]
                value = checkMissing(rowList[i])

                if variable == "cell_id":
                    if value in cellInfoDict: # At least one cell line is not in the cell_info file
                        sampleDict.update(cellInfoDict[value])

                elif variable == "pert_id":
                    if value in pertInfoDict:
                        sampleDict.update(pertInfoDict[value])

                    if value in pertMetricsDict:
                        sampleDict.update(pertMetricsDict[value])
                elif variable == "pert_time_unit":
                    continue # All values are the same

            for variable, value in sampleDict.items():
                 metaOut.write("{}\t{}\t{}\n".format(sample, variable, value).encode())
