import os, sys
from vedis import Vedis

inFilePath = sys.argv[1]
outFilePath = sys.argv[2]

if os.path.exists(outFilePath):
    os.remove(outFilePath)

with open(inFilePath) as inFile:
    db = Vedis(outFilePath)

    with db.transaction():
        inHeaderItems = inFile.readline().rstrip("\n").split("\t")

        samples = []
        for line in inFile:
            #lineItems = line.rstrip("\n").split("\t")
            line = line.rstrip("\n")
            firstTabIndex = line.index("\t")
            sample = line[:firstTabIndex]
            restOfLine = line[(firstTabIndex + 1):]

            samples.append(sample)

            db[sample] = restOfLine
#            l = db.List(sample)
#            l.extend(lineItems[1:])

            if len(samples) % 50000 == 0:
                print(len(samples))
                sys.stdout.flush()
                db.commit()
#                break

        db.commit()
    db.close()
