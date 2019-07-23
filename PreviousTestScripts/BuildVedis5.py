# TODO:
#   Try it with compression enabled
#   Calculate chunk sizes as number of data points rather than number of cols or rows
#   What do we do if there are multiple TSV files? Merge before/after?

import datetime, os, sys
from vedis import Vedis

inFilePath = sys.argv[1]
databaseFilePath = sys.argv[2]

#############
# Constants
#############

numSamplesPerSampleChunk = 50000
numSamplesPerTransposeChunk = 50000
numFeaturesPerConversionChunk = 500

#############
# Functions
#############

def smartPrint(x):
    print("{} - {}".format(datetime.datetime.now(), x))
    sys.stdout.flush()

#########################
smartPrint("Initialize")
#########################

if os.path.exists(databaseFilePath):
    os.remove(databaseFilePath)

db = Vedis(databaseFilePath)

sampleTable = db.Hash("samples")
featureTable = db.Hash("features")
metaTable = db.Hash("meta")

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
                #sampleTable.commit()
                #break

        #sampleTable.commit()

    ####################################
    smartPrint("Populate transposed table")
    ####################################

    featureDict = {}
    for feature in features:
        featureTable[feature] = ""
        featureDict[feature] = []
    #featureTable.commit()

    sampleIndices = list(range(len(samples)))

    samplesAdded = []
    while len(samplesAdded) < numSamplesPerTransposeChunk and len(sampleIndices) > 0:
        if len(sampleIndices) % numSamplesPerTransposeChunk == 0:
            smartPrint("{} samples remain to be processed".format(len(sampleIndices)))
            sys.stdout.flush()

        sampleIndex = sampleIndices.pop(0)
        sample = samples[sampleIndex]
        samplesAdded.append(sample)
        sampleData = sampleTable[sample].decode().split("\t")

        for i in range(len(sampleData)):
            feature = features[i]
            value = sampleData[i]
            featureDict[feature].append(value)

        if len(samplesAdded) == numSamplesPerTransposeChunk:
            for feature in features:
                featureTable[feature] = featureTable[feature].decode() + "\t".join(featureDict[feature]) + "\t"

            #featureTable.commit()
            samplesAdded = []

            featureDict = {}
            for feature in features:
                featureDict[feature] = []

    if len(samplesAdded) > 0:
        for feature in features:
            featureTable[feature] = featureTable[feature].decode() + "\t".join(featureDict[feature])
        #featureTable.commit()

    # It's helpful to store these so we know the order of the features and samples
    metaTable["features"] = features
    metaTable["samples"] = samples

    # It's helpful to store these so we can quickly get the index of a feature or sample
    metaTable["featuresDict"] = {x:i for i, x in enumerate(features)}
    metaTable["samplesDict"] = {x:i for i, x in enumerate(samples)}
    #metaTable.commit()

    #########################################################################
    # Used for testing purposes only
    #########################################################################
    #with open("/tmp/2.tsv", 'w') as transposedFile:
    #    transposedFile.write("\t".join([""] + samples) + "\n")
    #    for feature in features:
    #        transposedFile.write(feature + featureTable[feature].decode() + "\n")
    #########################################################################
finally:
    db.close()

smartPrint("Num features: {}".format(len(features)))
smartPrint("Num samples: {}".format(len(samples)))
