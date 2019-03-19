import sys, difflib

outputFilePath = sys.argv[1]
expectedOutputFilePath = sys.argv[2]

passed = True

def formatNumber(num):
    if "." in num or "e-" in num:
        return "{:.8f}".format(float(num))

    return num

with open(outputFilePath) as outputFile:
    with open(expectedOutputFilePath) as expectedOutputFile:
        line_count = 0

        for line in outputFile:
            line_count += 1
            line = line.rstrip("\n")
            expected_line = next(expectedOutputFile).rstrip("\n")

            line = "\t".join([formatNumber(x) for x in line.split("\t")])
            expected_line = "\t".join([formatNumber(x) for x in expected_line.split("\t")])

            if line != expected_line:
                print("{} and {} are not equal.".format(outputFilePath, expectedOutputFilePath))
                print("  Line {} of {}: {}".format(line_count, outputFilePath, line))
                print("  Line {} of {}: {}".format(line_count, expectedOutputFilePath, expected_line))
                passed = False
                break

        next_line = next(expectedOutputFile, "END_OF_FILE")
        if next_line != "END_OF_FILE":
            print("Some text was in {} that was not in {}.".format(expectedOutputFilePath, outputFilePath))
            passed = False

if line_count == 0:
    print("{} was empty.".format(outputFilePath))
    sys.exit(1)
else:
    if passed:
        print("Passed")
    else:
        sys.exit(1)
