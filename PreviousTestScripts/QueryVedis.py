import sys
from vedis import Vedis

inFilePath = sys.argv[1]
outFilePath = sys.argv[2]

with open(outFilePath, 'w') as outFile:
    db = Vedis(inFilePath)

    x = list(db.List("CPC005_A375_6H_X1_B3_DUO52HI53LO:K06"))
    y = list(db.List("PCLB003_PC3_24H_X3_B13:P15"))

    print(len(x))
    print(len(y))
