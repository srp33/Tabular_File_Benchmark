import sys, difflib

outputFilePath = sys.argv[1]
expectedOutputFilePath = sys.argv[2]

output = ""
with open(outputFilePath) as outputFile:
    for line in outputFile:
        output += line
    output = output.rstrip()

expectedOutput = ""
with open(expectedOutputFilePath) as expectedOutputFile:
    for line in expectedOutputFile:
        expectedOutput += line
    expectedOutput = expectedOutput.rstrip()

if output != expectedOutput:
    print("{} and {} are not equal.".format(outputFilePath, expectedOutputFilePath))
    sys.exit(1)




sys.exit(0)

diff = difflib.ndiff(output, expectedOutput)

numChars = 0.0
numDifferences = 0.0

for x in diff:
    print("got here")
    sys.exit(0)
    sign = x[0]
    character = x[2]

    numChars += 1

    if sign != " ":
        numDifferences += 1
        print(numDifferences)

if numDifferences > 0:
    print("\n#######################################################")
    print("# Output:")
    print("#######################################################\n")
    print(output)
    print("\n#######################################################")
    print("# Expected output:")
    print("#######################################################\n")
    print(expectedOutput)
