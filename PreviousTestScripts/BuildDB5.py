# TODO:
#   Calculate chunk sizes as number of data points rather than number of cols or rows
#   What do we do if there are multiple TSV files? Merge before/after?

import datetime, os, sys
from sqlitedict import SqliteDict
import zlib, pickle, sqlite3

inFilePath = sys.argv[1]
tempDatabaseFilePath = sys.argv[2]
compressedDatabaseFilePath = sys.argv[3]

#############
# Constants
#############

autocommit = False
journal_mode = "OFF"
numSamplesPerSampleChunk = 50000
numSamplesPerTransposeChunk = 50000
numFeaturesPerConversionChunk = 500

#############
# Functions
#############

def my_encode(obj):
    return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))

def my_decode(obj):
    return pickle.loads(zlib.decompress(bytes(obj)))

def getDbTable(filePath, tableName, compressed):
    if compressed:
        return SqliteDict(filePath, autocommit=autocommit, journal_mode=journal_mode, tablename=tableName, encode=my_encode, decode=my_decode)
    else:
        return SqliteDict(filePath, autocommit=autocommit, journal_mode=journal_mode, tablename=tableName)

def smartPrint(x):
    print("{} - {}".format(datetime.datetime.now(), x))
    sys.stdout.flush()

#########################
smartPrint("Initialize")
#########################

if os.path.exists(tempDatabaseFilePath):
    os.remove(tempDatabaseFilePath)
if os.path.exists(compressedDatabaseFilePath):
    os.remove(compressedDatabaseFilePath)

sampleTable = getDbTable(tempDatabaseFilePath, "samples", False)
featureTable = getDbTable(tempDatabaseFilePath, "features", False)
sampleTableCompressed = getDbTable(compressedDatabaseFilePath, "samples", True)
featureTableCompressed = getDbTable(compressedDatabaseFilePath, "features", True)
metaTableCompressed = getDbTable(compressedDatabaseFilePath, "meta", True)

samples = []

###########################################
smartPrint("Pull samples from TSV into DB")
###########################################

try:
    with open(inFilePath) as inFile:
        features = inFile.readline().rstrip("\n").split("\t")[1:]

        for line in inFile:
            line = line.rstrip("\n")
            firstTabIndex = line.index("\t")
            sample = line[:firstTabIndex]
            restOfLine = line[(firstTabIndex + 1):]

            samples.append(sample)
            sampleTable[sample] = restOfLine

            if len(samples) % numSamplesPerSampleChunk == 0:
                smartPrint("Sample {}".format(len(samples)))
                sys.stdout.flush()
                sampleTable.commit()
                #break

        sampleTable.commit()

    ####################################
    smartPrint("Populate transposed table")
    ####################################

    featureDict = {}
    for feature in features:
        featureTable[feature] = ""
        featureDict[feature] = []
    featureTable.commit()

    sampleIndices = list(range(len(samples)))

    samplesAdded = []
    while len(samplesAdded) < numSamplesPerTransposeChunk and len(sampleIndices) > 0:
        if len(sampleIndices) % numSamplesPerTransposeChunk == 0:
            smartPrint("{} samples remain to be processed".format(len(sampleIndices)))
            sys.stdout.flush()

        sampleIndex = sampleIndices.pop(0)
        sample = samples[sampleIndex]
        samplesAdded.append(sample)
        sampleData = sampleTable[sample].split("\t")

        for i in range(len(sampleData)):
            feature = features[i]
            value = sampleData[i]
            featureDict[feature].append(value)

        if len(samplesAdded) == numSamplesPerTransposeChunk:
            for feature in features:
                featureTable[feature] = featureTable[feature] + "\t".join(featureDict[feature]) + "\t"

            featureTable.commit()
            samplesAdded = []

            featureDict = {}
            for feature in features:
                featureDict[feature] = []

    if len(samplesAdded) > 0:
        for feature in features:
            featureTable[feature] = featureTable[feature] + "\t".join(featureDict[feature])
        featureTable.commit()

    #########################################################
    smartPrint("Converting sample info to compressed lists")
    #########################################################

    for i in range(len(samples)):
        if i > 0 and i % numSamplesPerSampleChunk == 0:
            smartPrint(i)
            sampleTable.commit()
            sampleTableCompressed.commit()

        sample = samples[i]
        sampleTableCompressed[sample] = sampleTable[sample].split("\t")
    sampleTable.commit()
    sampleTableCompressed.commit()

    ########################################################
    smartPrint("Converting features to compressed lists")
    ########################################################

    for i in range(len(features)):
        if i > 0 and i % numFeaturesPerConversionChunk == 0:
            smartPrint(i)
            featureTable.commit()
            featureTableCompressed.commit()

        feature = features[i]
        featureTableCompressed[feature] = featureTable[feature].split("\t")
    featureTable.commit()
    featureTableCompressed.commit()

    ########################################################
    smartPrint("Store metadata")
    ########################################################

    # It's helpful to store these so we know the order of the features and samples
    metaTableCompressed["features"] = features
    metaTableCompressed["samples"] = samples

    # It's helpful to store these so we can quickly get the index of a feature or sample
    metaTableCompressed["featuresDict"] = {x:i for i, x in enumerate(features)}
    metaTableCompressed["samplesDict"] = {x:i for i, x in enumerate(samples)}
    metaTableCompressed.commit()

    #########################################################################
    # Used for testing purposes only
    #########################################################################
    #with open("/tmp/1.tsv", 'w') as transposedFile:
    #    transposedFile.write("\t".join([""] + samples) + "\n")
    #    for feature in features:
    #        transposedFile.write("\t".join([feature] + featureTableCompressed[feature]) + "\n")
    #########################################################################
finally:
    sampleTable.close()
    featureTable.close()
    sampleTableCompressed.close()
    featureTableCompressed.close()
    metaTableCompressed.close()

smartPrint("Num features: {}".format(len(features)))
smartPrint("Num samples: {}".format(len(samples)))

os.remove(tempDatabaseFilePath)
