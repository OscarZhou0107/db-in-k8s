import argparse
import csv
import gzip
import itertools
import math
import os
import statistics
from datetime import datetime, timedelta

try:
    from dateutil import parser as dateutil_parser
except:
    print('Error:', 'pip install python-dateutil')


__help__ = 'Parsing for a single run'


def geomean(data):
    return math.exp(math.fsum(math.log(x) for x in data) / len(data))


def construct_filter(request_type, request_result):
    def request_filter(row):
        if request_type is None:
            if request_result is None:
                None
            else:
                return row['request_result'] == request_result
        else:
            if request_result is None:
                return row['request_type'] == request_type
            else:
                return row['request_result'] == request_result and row['request_type'] == request_type
    return request_filter


def get_request_type_list():
    return ['BeginTx', 'Commit', 'Rollback', 'ReadOnly', 'WriteOnly', 'ReadOnlyEarlyRelease', 'WriteOnlyEarlyRelease', 'SingleReadOnly', 'SingleWriteOnly', None]


def get_request_result_list():
    return ['Ok', 'Err', None]


def successful_request_filter(row):
    return row['request_result'] == 'Ok'


class DBRow(dict):
    def __init___(self, row):
        super(DBRow, self).__init__(row)

    def pretty_print_row(self):
        modified_row = self.copy()
        modified_row['initial_timestamp'] = modified_row['initial_timestamp'].isoformat()
        modified_row['final_timestamp'] = modified_row['final_timestamp'].isoformat()
        print('Info:', *map(lambda kv: (str(kv[0]), str(kv[1])), modified_row.items()))


class OpenAnyFile():
    def __init__(self, path):
        self.path = path

    def __enter__(self):
        if self.path.endswith('gz'):
            self.fd = gzip.open(self.path, 'rt')
        else:
            self.fd = open(self.path)
        return self.fd

    def __exit__(self, type, value, traceback):
        self.fd.close()


class DB(list):
    def __init__(self, csv_path=None, data=None, debug=False):
        if csv_path is not None:
            with OpenAnyFile(csv_path) as csvfile:
                if debug:
                    print('Info:', 'Parsing', csv_path)
                csvreader = csv.DictReader(csvfile)
                super(DB, self).__init__(csvreader)
        elif data is not None:
            super(DB, self).__init__(data)

    def pretty_print(self):
        for row in self:
            DBRow(row).pretty_print_row()


class PerfDB(DB):
    def __init__(self, perf_csv_path=None, data=None):
        if perf_csv_path is not None:
            super(PerfDB, self).__init__(csv_path=perf_csv_path, data=data)
            for row in self:
                row['initial_timestamp'] = dateutil_parser.isoparse(row['initial_timestamp'])
                row['final_timestamp'] = dateutil_parser.isoparse(row['final_timestamp'])
                row['latency'] = (row['final_timestamp'] - row['initial_timestamp']).total_seconds()
        elif data is not None:
            super(PerfDB, self).__init__(data=data)

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
        first_timestamp = min(db, key=lambda row: row['initial_timestamp'])['initial_timestamp']
        print('Info:', 'first_timestamp:', first_timestamp)

        # Sort all rows by final_timestamp
        db = sorted(db, key=lambda row: row['final_timestamp'])
        grouped_by_sec = itertools.groupby(db, key=lambda row: int((row['final_timestamp']-first_timestamp)/interval_length))

        groups_of_rows = list()
        secs = list()
        for sec, rows in grouped_by_sec:
            groups_of_rows.append(sorted(rows, key=lambda row: row['final_timestamp']))
            secs.append(sec)

        return Throughput(zip(secs, groups_of_rows))

    def get_latency_stats(self, filter_func=None):
        '''
        in seconds
        (mean, stddev, geomean, median)
        '''
        db = list(self)
        if filter_func is not None:
            db = list(filter(filter_func, db))

        if len(db) < 2:
            return

        latency = list(map(lambda row: row['latency'], db))
        return (statistics.mean(latency), statistics.stdev(latency), geomean(latency), statistics.median(latency))

    def get_filtered(self, filter_func):
        return PerfDB(data=list(filter(filter_func, self)))


class Throughput(list):
    '''
    return [(len_after_first_group, rows)]
    sorted by len_after_first_group, and len_after_first_group is unique throughout the list
    rows are also sorted based on final_timestamp
    '''

    def get_trajectory(self):
        return list(map(lambda id_rows: (id_rows[0], len(id_rows[1])), self))

    def get_stats(self):
        '''
        (peak, mean, stddev, geomean, median)
        '''
        values = list(map(lambda kv: kv[1], self.get_trajectory()))
        return (max(values), statistics.mean(values), statistics.stdev(values), geomean(values), statistics.median(values))

    def print_trajectory(self):
        print('Info:')
        print('Info:', 'Throughput(#request_finished/sec) Trajectory')
        trajectory = self.get_trajectory()
        for item in trajectory:
            print('Info:', item)
        print('Info:')
        print('Info:', 'Peak throughput is', max(trajectory, key=lambda kv: kv[1]))

    def print_detailed_trajectory(self):
        for (sec, group_of_rows) in self:
            print('Info:')
            print('Info:', sec)
            for row in group_of_rows:
                DBRow(row).pretty_print_row()


class DbproxyStatsDB(DB):
    def __init__(self, dbproxy_stats_csv_path):
        super(DbproxyStatsDB, self).__init__(csv_path=dbproxy_stats_csv_path)

    def get_num_dbproxy(self):
        return len(self)

    def print_data(self):
        print('Info:', 'Dbproxy Stats')
        for row in self:
            print('Info:', *row.values())


def print_perfdb_latency_stats(perfdb):
    '''
    Expecting an unfiltered perfdb
    '''
    # For each request    
    print('Info:', 'Request Latency Stats - Sec / Request')
    for request_type in get_request_type_list():
        for request_result in get_request_result_list():
            res = perfdb.get_latency_stats(filter_func=construct_filter(request_type, request_result))
            if res is not None:
                mean, stddev, geomean, median = res

                request_type = 'All' if request_type is None else request_type
                request_result = 'All' if request_result is None else request_result
                print('Info:', '{:<27} {:<6} [Mean: {:>4.2f}] [SD: {:>4.2f}] [Geomean: {:>4.2f}] [Median: {:>4.2f}]'.format(request_type, request_result, mean, stddev, geomean, median))


def print_perfdb_success_ratio_stats(perfdb):
    '''
    Expecting an unfiltered perfdb
    '''
    # For each request    
    print('Info:', 'Request Result Stats')
    for request_type in get_request_type_list():
        num_ok = len(perfdb.get_filtered(filter_func=construct_filter(request_type, 'Ok')))
        num_err = len(perfdb.get_filtered(filter_func=construct_filter(request_type, 'Err')))
        request_type = 'All' if request_type is None else request_type
        print('Info:', '{:<27} {:>7} Ok {:>7} Err {:>9.2f} %'.format(request_type, num_ok, num_err, (num_ok/(num_err+num_ok) if num_ok+num_err > 0 else 1) * 100.0))


def init(parser):
    parser.add_argument('log_dir', type=str, help='log file directory for single run')


def main(args):
    # Parse perf csv
    perfdb = PerfDB(perf_csv_path=os.path.join(args.log_dir, 'perf.csv.gz'))

    # Parse dbproxy stats csv
    dbproxy_stats_db = DbproxyStatsDB(os.path.join(args.log_dir, 'dbproxy_stats.csv.gz'))

    # Apply filter on perfdb
    sr_perfdb = perfdb.get_filtered(successful_request_filter)

    sr_throughput = sr_perfdb.get_throughput()
    # sr_throughput.print_detailed_trajectory()
    # sr_throughput.print_trajectory()
    print('Info:')
    print('Info:', 'Successful Request:', len(sr_perfdb))
    print('Info:')
    print('Info:', 'Throughput - Request Completed / Sec')
    print('Info:', '(peak, mean, stddev, geomean, median)')
    print('Info:', sr_throughput.get_stats())
    print('Info:')
    print('Info:', 'Latency - Sec / Request')
    print('Info:', '(mean, stddev, geomean, median)')
    print('Info:', sr_perfdb.get_latency_stats())

    print('Info:')
    print('Info:', 'num_clients', sr_perfdb.get_num_clients())
    print('Info:', 'num_dbproxy_db', dbproxy_stats_db.get_num_dbproxy())

    print('Info:')
    print_perfdb_latency_stats(perfdb)

    print('Info:')
    print_perfdb_success_ratio_stats(perfdb)

    print('Info:')
    dbproxy_stats_db.print_data()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    init(parser)
    main(parser.parse_args())
