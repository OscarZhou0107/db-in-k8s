import argparse
import csv
import itertools
import os
from datetime import datetime, timedelta

try:
    from dateutil import parser as dateutil_parser
except:
    print('Error:', 'pip install python-dateutil')


def parse_csv(csv_path):
    '''
    [{key:value}]
    '''

# [{key: value}]


class DBRow(dict):
    def __init___(self, row):
        super(DBRow, self).__init__(row)

    def pretty_print_row(self):
        modified_row = self.copy()
        modified_row['initial_timestamp'] = modified_row['initial_timestamp'].isoformat(
        )
        modified_row['final_timestamp'] = modified_row['final_timestamp'].isoformat()
        print(
            'Info:', *map(lambda kv: (str(kv[0]), str(kv[1])), modified_row.items()))


class DB(list):
    def __init__(self, csv_path):
        with open(csv_path) as csvfile:
            print('Info:', 'Parsing', csv_path)
            csvreader = csv.DictReader(csvfile)
            super(DB, self).__init__(csvreader)

    def pretty_print(self):
        for row in self:
            DBRow(row).pretty_print_row()


class PerfDB(DB):
    def __init__(self, perf_csv_path):
        super(PerfDB, self).__init__(perf_csv_path)
        for row in self:
            row['initial_timestamp'] = dateutil_parser.isoparse(
                row['initial_timestamp'])
            row['final_timestamp'] = dateutil_parser.isoparse(
                row['final_timestamp'])
            row['latency'] = row['final_timestamp'] - row['initial_timestamp']

    def get_num_clients(self):
        return len(set(map(lambda x: x['client_addr'], self)))

    def get_throughput(self, filter_func=None, interval_length=timedelta(seconds=1)):
        '''
        filter_func(row) returns false to ignore the row if filter_func is not None

        A list of groups, each group is defined to be
        having final_timestamp within [k, k+1] integer multiple of interval_length since the earliest initial_timestamp

        return Throughput
        '''

        db = list(self)
        if filter_func is not None:
            db = list(filter(filter_func, db))

        if len(db) == 0:
            return

        # Base is set to be the earliest initial_timestamp
        first_timestamp = min(db, key=lambda row: row['initial_timestamp'])[
            'initial_timestamp']
        print('Info:', 'first_timestamp:', first_timestamp)

        # Sort all rows by final_timestamp
        db = sorted(db, key=lambda row: row['final_timestamp'])
        grouped_by_sec = itertools.groupby(db, key=lambda row: int(
            (row['final_timestamp']-first_timestamp)/interval_length))

        groups_of_rows = list()
        secs = list()
        for sec, rows in grouped_by_sec:
            groups_of_rows.append(
                sorted(rows, key=lambda row: row['final_timestamp']))
            secs.append(sec)

        return Throughput(zip(secs, groups_of_rows))


class Throughput(list):
    '''
    return [(len_after_first_group, rows)]
    sorted by len_after_first_group, and len_after_first_group is unique throughout the list
    rows are also sorted based on final_timestamp
    '''

    def get_trajectory(self):
        return list(map(lambda id_rows: (id_rows[0], len(id_rows[1])), self))

    def print_trajectory(self):
        print('Info:')
        print('Info:', 'Throughput(#request_finished/sec) Trajectory')
        trajectory = self.get_trajectory()
        for item in trajectory:
            print('Info:', item)
        print('Info:')
        print('Info:', 'Peak throughput is', max(
            trajectory, key=lambda kv: kv[1]))

    def print_detailed_trajectory(self):
        for (sec, group_of_rows) in self:
            print('Info:')
            print('Info:', sec)
            for row in group_of_rows:
                DBRow(row).pretty_print_row()


class DbproxyStatsDB(DB):
    def __init__(self, dbproxy_stats_csv_path):
        super(DbproxyStatsDB, self).__init__(dbproxy_stats_csv_path)

    def get_num_dbproxy(self):
        return len(self)


def init(parser):
    parser.add_argument('--log_dir', type=str, required=True, help='log file')


def main(args):
    # Parse perf csv
    perfdb = PerfDB(os.path.join(args.log_dir, 'perf.csv'))

    # Parse dbproxy stats csv
    dbproxy_stats_db = DbproxyStatsDB(
        os.path.join(args.log_dir, 'dbproxy_stats.csv'))

    # Find the throughput for all time intervals since the beginning
    all_throughput = perfdb.get_throughput()
    print('Info:', 'All Throughput:')
    # all_throughput.print_detailed_trajectory()
    all_throughput.print_trajectory()

    successful_throughput = perfdb.get_throughput(
        filter_func=lambda row: row['request_result'] == 'Ok')
    print('Info:', 'Successful Request Throughput:')
    successful_throughput.print_trajectory()

    successful_query_throughput = perfdb.get_throughput(filter_func=lambda row: row['request_result'] in [
        'ReadOnly', 'WriteOnly', 'ReadOnlyEarlyRelease', 'WriteOnlyEarlyRelease'])
    print('Info:', 'Successful Query Request Throughput:')
    successful_query_throughput.print_trajectory()

    num_clients = perfdb.get_num_clients()
    num_dbproxy_db = dbproxy_stats_db.get_num_dbproxy()
    print('Info:', 'num_clients', num_clients)
    print('Info:', 'num_dbproxy_db', num_dbproxy_db)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    init(parser)
    main(parser.parse_args())
