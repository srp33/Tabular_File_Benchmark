import sys, difflib

outputFilePath = sys.argv[1]
expectedOutputFilePath = sys.argv[2]

passed = True

with open(outputFilePath) as outputFile:
    with open(expectedOutputFilePath) as expectedOutputFile:
        line_count = 0

        for line in outputFile:
            line_count += 1
            line = line.rstrip("\n")
            expected_line = next(expectedOutputFile).rstrip("\n")

            line = "\t".join([x.rstrip("0") for x in line.split("\t")])
            expected_line = "\t".join([x.rstrip("0") for x in expected_line.split("\t")])

            if line != expected_line:
                print("{} and {} are not equal.".format(outputFilePath, expectedOutputFilePath))
                print("  Line {} of {}: {}".format(line_count, outputFilePath, line))
                print("  Line {} of {}: {}".format(line_count, expectedOutputFilePath, expected_line))
                passed = False
                break

if passed:
    print("Passed")
