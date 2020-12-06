import csv
from datetime import datetime
try:
    from dateutil import parser
except:
    print('pip install python-dateutil')


def parse_csv(log_path):
    '''
    [{key:value}]
    '''
    with open(log_path) as csvfile:
        csvreader = csv.DictReader(csvfile)
        return list(csvreader)


def prepare_db(db):
    def convert_db(db):
        for row in db:
            row['initial_timestamp'] = parser.isoparse(row['initial_timestamp'])
            row['final_timestamp'] = parser.isoparse(row['final_timestamp'])

    def append_latency_db(db):
        for row in db:
            row['latency'] = row['final_timestamp'] - row['initial_timestamp']
    
    convert_db(db)
    append_latency_db(db)


def calculate_throughput(db):
    '''
    [num_request_completed@sec0, num_request_completed@sec1, num_request_completed@sec2]
    '''
    pass

def main():
    db = parse_csv('o2versioner/logging/perf_201206_072034.csv')
    prepare_db(db)

    for row in db:
        print(row)


if __name__ == '__main__':
    main()
