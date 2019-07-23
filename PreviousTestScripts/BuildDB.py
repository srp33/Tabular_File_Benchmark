# TODO:
#   Try it with compression enabled
#   Calculate chunk sizes as number of data points rather than number of cols or rows
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
numSamplesPerSampleChunk = 50000
numSamplesPerTransposeChunk = 50000

#############
# Functions
#############

#def my_encode(obj):
#    return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))

#def my_decode(obj):
#    return pickle.loads(zlib.decompress(bytes(obj)))

def getDbTable(tableName):
    return SqliteDict(databasePath, autocommit=autocommit, journal_mode=journal_mode)

################
# Initialize
################

if os.path.exists(databasePath):
    os.remove(databasePath)

sampleTable = getDbTable("samples")
featureTable = getDbTable("features")
metaTable = getDbTable("meta")

samples = []

#################################
# Pull samples from TSV into DB
#################################

try:
    with open(inFilePath) as inFile:
        features = inFile.readline().rstrip("\n").split("\t")[1:]

        for line in inFile:
            lineItems = line.rstrip("\n").split("\t")

            sample = lineItems[0]
            samples.append(sample)

            sampleTable[sample] = lineItems[1:]

            if len(samples) % numSamplesPerSampleChunk == 0:
                print("Sample {}".format(len(samples)))
                sys.stdout.flush()
                sampleTable.commit()
                #break

        sampleTable.commit()

    #################################
    # Populate transposed table
    #################################

    featureDict = {}
    for feature in features:
        featureTable[feature] = []
        featureDict[feature] = []
    featureTable.commit()

    sampleIndices = list(range(len(samples)))

    samplesAdded = []
    while len(samplesAdded) < numSamplesPerTransposeChunk and len(sampleIndices) > 0:
        if len(sampleIndices) % numSamplesPerTransposeChunk == 0:
            print("{} samples remain to be processed".format(len(sampleIndices)))
            sys.stdout.flush()

        sampleIndex = sampleIndices.pop(0)
        sample = samples[sampleIndex]
        samplesAdded.append(sample)
        sampleData = sampleTable[sample]

        for i in range(len(sampleData)):
            feature = features[i]
            value = sampleData[i]
            featureDict[feature].append(value)

        if len(samplesAdded) == numSamplesPerTransposeChunk:
            for feature in features:
                featureTable[feature] = featureTable[feature] + featureDict[feature]

            featureTable.commit()
            samplesAdded = []

            featureDict = {}
            for feature in features:
                featureDict[feature] = []

    if len(samplesAdded) > 0:
        for feature in features:
            featureTable[feature] = featureTable[feature] + featureDict[feature]
        featureTable.commit()

    metaTable["features"] = features
    metaTable["samples"] = samples
    metaTable["featuresDict"] = {x:i for i,x in enumerate(features)}
    metaTable["samplesDict"] = {x:i for i,x in enumerate(samples)}
    metaTable.commit()

    #########################################################################
    # Used for testing purposes only
    #########################################################################
    #with open("/tmp/1.tsv", 'w') as transposedFile:
    #    transposedFile.write("\t".join([""] + samples) + "\n")
    #    for feature in features:
    #        transposedFile.write("\t".join([feature] + featureTable[feature]) + "\n")
    #########################################################################
finally:
    sampleTable.close()
    featureTable.close()
    metaTable.close()

print("Num features: {}".format(len(features)))
print("Num samples: {}".format(len(samples)))
