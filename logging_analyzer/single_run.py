import csv

def parse_file(log_path):
    with open(log_path) as csvfile:
        csvreader = csv.DictReader(csvfile)
        for line in csvreader:
            print(line)


parse_file('o2versioner/logging/perf_201206_072034.csv')