# TODO:
#   Try it with compression enabled
#   Calculate chunk sizes as number of data points rather than number of cols or rows
#   What do we do if there are multiple TSV files? Merge before/after?

import datetime, os, sys
import shutil
import json
import plyvel

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
    shutil.rmtree(databaseFilePath)

db = plyvel.DB(databaseFilePath, create_if_missing=True)

sampleTable = db.prefixed_db(b"samples")
featureTable = db.prefixed_db(b"features")
metaTable = db.prefixed_db(b"meta")

samples = []

###########################################
smartPrint("Pull samples from TSV into DB")
###########################################

try:
    with open(inFilePath) as inFile:
        features = inFile.readline().rstrip("\n").split("\t")[1:]
        sampleWB = sampleTable.write_batch()
        for line in inFile:
            line = line.rstrip("\n")
            firstTabIndex = line.index("\t")
            sample = line[:firstTabIndex]
            restOfLine = line[(firstTabIndex + 1):]

            samples.append(sample)
            sampleWB.put(sample.encode(), restOfLine.encode())

            if len(samples) % numSamplesPerSampleChunk == 0:
                smartPrint("Sample {}".format(len(samples)))
                sys.stdout.flush()
                #sampleTable.commit()
                #break
        sampleWB.write()
        #sampleTable.commit()

    ####################################
    smartPrint("Populate transposed table")
    ####################################

    featureDict = {}
    featureWB = featureTable.write_batch()
    for feature in features:
        featureWB.put(feature.encode(), b"")
        featureDict[feature] = []
    featureWB.write()
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
        sampleData = sampleTable.get(sample.encode()).decode().split("\t")

        for i in range(len(sampleData)):
            feature = features[i]
            value = sampleData[i]
            featureDict[feature].append(value)

        if len(samplesAdded) == numSamplesPerTransposeChunk:
            for feature in features:
                featureWB.put(feature.encode(), (featureTable.get(feature.encode()).decode() + "\t".join(featureDict[feature]) + "\t").encode())
            featureWB.write()
            #featureTable.commit()
            samplesAdded = []

            featureDict = {}
            for feature in features:
                featureDict[feature] = []

    if len(samplesAdded) > 0:
        for feature in features:
            featureWB.put(feature.encode(), (featureTable.get(feature.encode()).decode() + "\t".join(featureDict[feature])).encode())
        featureWB.write()
        #featureTable.commit()

    # It's helpful to store these so we know the order of the features and samples
    metaTable.put(b"features", ', '.join(features).encode())
    metaTable.put(b"samples", ', '.join(samples).encode())

    # It's helpful to store these so we can quickly get the index of a feature or sample
    metaTable.put(b"featuresDict", json.dumps({x:i for i, x in enumerate(features)}).encode())
    metaTable.put(b"samplesDict", json.dumps({x:i for i, x in enumerate(samples)}).encode())
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
