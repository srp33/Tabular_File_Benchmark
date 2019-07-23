with open("/Applications/tmp/Gene_Expression.tsv") as theFile:
    for i in range(50000):
        theFile.seek(10*i)
        line = theFile.readline()
