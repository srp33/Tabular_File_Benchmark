import random, sys
from sqlitedict import SqliteDict
import zlib, pickle, sqlite3

databaseFilePath = sys.argv[1]
outFilePath = sys.argv[2]

#############
# Functions
#############

def my_encode(obj):
    return sqlite3.Binary(zlib.compress(pickle.dumps(obj, pickle.HIGHEST_PROTOCOL)))

def my_decode(obj):
    return pickle.loads(zlib.decompress(bytes(obj)))

def getDbTable(tableName):
    return SqliteDict(databaseFilePath, tablename=tableName, flag="r", encode=my_encode, decode=my_decode)
    #return SqliteDict(databaseFilePath, tablename=tableName, flag="r")

def smartPrint(x):
    print("{} - {}".format(datetime.datetime.now(), x))
    sys.stdout.flush()

#########################
# Initialize connections
#########################

sampleTable = getDbTable("samples")
featureTable = getDbTable("features")
metaTable = getDbTable("meta")

with open(outFilePath, 'w') as outFile:
    outFile.write("\t".join(["Sample"] + metaTable["features"]) + "\n")

    samples = metaTable["samples"]
    random.shuffle(samples)
    samples = set(samples[:50000])

    sampleCount = 0
#    for sample, data in sampleTable.iteritems():
#    for sample in sampleTable.keys():
    for sample in samples:
        data = sampleTable[sample]
        sampleCount += 1
        if sampleCount % 1000 == 0:
            print(sampleCount)

#        if sample in samples:
        outFile.write(sample + data + "\n")
