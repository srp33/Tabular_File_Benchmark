import datetime, random, sys
from randomAccessReader import CsvRandomAccessReader

inFilePath = sys.argv[1]
numRandom = int(sys.argv[2])
#outFilePath = sys.argv[3]

def smartPrint(x):
    print("{} - {}".format(datetime.datetime.now(), x))
    sys.stdout.flush()

smartPrint("Building random access reader")
reader = CsvRandomAccessReader(inFilePath, values_delimiter="\t")

lineNums = list(range(numRandom))
random.shuffle(lineNums)

for i in range(len(lineNums)):
    if i % 1000 == 0:
        smartPrint(i)

    x = reader.get_lines(lineNums[i])
