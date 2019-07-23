# TODO:
#   Calculate chunk sizes as number of data points rather than number of cols or rows
#   Try it with compression enabled
#   What do we do if there are multiple TSV files? Merge before/after?

import os, sys
from sqlitedict import SqliteDict
import zlib, pickle, sqlite3

inFilePath = sys.argv[1]
databasePath = sys.argv[2]

#############
# Constants
#############

autocommit = False
journal_mode = "OFF"
numSamplesChunk = 5000
numFeaturesChunk = 1000

#############
# Functions
#############

#def my_encode(obj):
#    return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))

#def my_decode(obj):
#    return pickle.loads(zlib.decompress(bytes(obj)))

def getDbTable(tableName):
    return SqliteDict(databasePath, autocommit=autocommit, journal_mode=journal_mode)

def printNow(message):
    print(message)
    sys.stdout.flush()

####################################################
printNow("Initializing database...")
####################################################

if os.path.exists(databasePath):
    os.remove(databasePath)

sampleTable = getDbTable("samples")
featureTable = getDbTable("features")
metaTable = getDbTable("meta")

samples = []
features = []

####################################################
printNow("Identifying features and samples...")
####################################################

lineCount = 0
with open(inFilePath) as inFile:
    features = inFile.readline().rstrip("\n").split("\t")[1:]

    for line in inFile:
        lineCount +=1
        if lineCount % numSamplesChunk == 0:
            printNow("Line {}".format(lineCount))

        lineItems = line.split("\t")
        samples.append(lineItems[0])

printNow("Num features: {}".format(len(features)))
printNow("Num samples: {}".format(len(samples)))

####################################################
printNow("Initialize database tables...")
####################################################

try:
    metaTable["features"] = {k: v for v, k in enumerate(features)}
    metaTable["samples"] = {k: v for v, k in enumerate(samples)}
    metaTable.commit()

    featureCount = 0
    for feature in features:
        featureCount += 1
        if featureCount % numFeaturesChunk == 0:
            printNow("Feature {}".format(featureCount))

        featureTable[feature] = {}
    featureTable.commit()

    sampleCount = 0
    for sample in samples:
        sampleCount += 1
        if sampleCount % numSamplesChunk == 0:
            printNow("Sample {}".format(sampleCount))

        sampleTable[sample] = {}
    sampleTable.commit()

####################################################
    printNow("Populate database tables...")
####################################################

    with open(inFilePath) as inFile:
        inFile.readline()

        sampleCount = 0
        for line in inFile:
            sampleCount += 1

            lineItems = line.rstrip("\n").split("\t")
            sample = lineItems.pop(0)

            for i in range(len(lineItems)):
                feature = features[i]
                value = lineItems[i]

                sampleTable[sample][feature] = value
                featureTable[feature][sample] = value

            if sampleCount % numSamplesChunk == 0:
                printNow("Sample {}".format(sampleCount))
                sampleTable.commit()
                featureTable.commit()

    sampleTable.commit()
    featureTable.commit()
finally:
    sampleTable.close()
    featureTable.close()
    metaTable.close()
