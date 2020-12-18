import argparse
import csv
import gzip
import itertools
import math
import os
import statistics
from datetime import datetime, timedelta

try:
    import pandas as pd
except:
    print('Error:', 'pip install pandas')

try:
    from dateutil import parser as dateutil_parser
except:
    print('Error:', 'pip install python-dateutil')

try:
    import matplotlib.pyplot as plt
except:
    print('Error:', 'pip install matplotlib')


__help__ = 'Parsing for a single run'


def geomean(data):
    return math.exp(math.fsum(math.log(x) for x in data) / len(data))


def construct_filter(request_type=None, request_result=None):
    def op(value, against):
        assert(type(value) == str)

        if type(against) == str:
            return value == against
        elif type(against) == list:
            return value in against
        else:
            assert(0)

    def request_filter(row):
        if request_type is None:
            if request_result is None:
                None
            else:
                return op(row['request_result'], request_result)
        else:
            if request_result is None:
                return op(row['request_type'], request_type)
            else:
                return op(row['request_result'], request_result) and op(row['request_type'], request_type)
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

    def get_throughput(self, interval_length=timedelta(seconds=0.1)):
        '''
        A list of groups, each group is defined to be
        having final_timestamp within [k, k+1] integer multiple of interval_length since the earliest initial_timestamp

        return Throughput
        '''

        db = list(self)

        if len(db) == 0:
            return

        # Base is set to be the earliest initial_timestamp
        first_timestamp = min(db, key=lambda row: row['initial_timestamp'])['initial_timestamp']
        # print('Info:', 'first_timestamp:', first_timestamp)

        # Sort all rows by final_timestamp
        db = sorted(db, key=lambda row: row['final_timestamp'])
        grouped_by_sec = itertools.groupby(db, key=lambda row: int((row['final_timestamp']-first_timestamp)/interval_length))

        groups_of_rows = list()
        secs = list()
        for sec, rows in grouped_by_sec:
            groups_of_rows.append(sorted(rows, key=lambda row: row['final_timestamp']))
            secs.append(sec)

        return Throughput(data=zip(secs, groups_of_rows), interval_length=interval_length)

    def get_latency_stats(self):
        '''
        in seconds
        (mean, stddev, geomean, median)
        '''
        db = list(self)

        if len(db) < 2:
            return

        latency = list(map(lambda row: row['latency'], db))
        return (statistics.mean(latency), statistics.stdev(latency), geomean(latency), statistics.median(latency))

    def plot_latency_distribution(self, ax, alpha=1, label=None, bins=50, log=True):
        db = list(self)

        if len(db) == 0:
            return
        
        latencies = list(map(lambda row: row['latency'], db))
        ax.hist(latencies, bins=bins, edgecolor='black', alpha=alpha, label=label, log=log)
        ax.set(xlabel='Latency (Sec/RequestFinished)', ylabel='Frequency')

    def get_filtered(self, filter_func):
        return PerfDB(data=list(filter(filter_func, self)))


class Throughput(list):
    '''
    [(len_after_first_group, [row])]
    sorted by len_after_first_group, and len_after_first_group is unique throughout the list
    rows are also sorted based on final_timestamp
    '''
    def __init__(self, data, interval_length):
        super().__init__(data)
        self._interval_length = interval_length

    def print_detailed_trajectory(self):
        for (sec, group_of_rows) in self:
            print('Info:')
            print('Info:', sec)
            for row in group_of_rows:
                DBRow(row).pretty_print_row()
                
    def get_trajectory(self):
        '''
        [(len_after_first_group, nrows)]
        or number of requests completed at len_after_first_group th group
        '''
        return list(map(lambda id_rows: (id_rows[0], len(id_rows[1])), self))

    def print_trajectory(self):
        print('Info:')
        print('Info:', 'Throughput Trajectory')
        trajectory = self.get_trajectory()
        for item in trajectory:
            print('Info:', item)
        print('Info:')
        print('Info:', 'Peak throughput is', max(trajectory, key=lambda kv: kv[1]))

    def get_throughputs_per_sec(self, window=10):
        base_throughputs = tuple(zip(*self.get_trajectory()))[1]
        multiplier = timedelta(seconds=1) / self._interval_length
        raw = list(map(lambda t: multiplier * t, base_throughputs))
        return list(pd.Series(raw).rolling(window=window).mean().iloc[window-1:].values)

    def get_stats(self):
        '''
        (peak, mean, stddev, geomean, median)
        '''
        values = self.get_throughputs_per_sec(window=1)
        return (max(values), statistics.mean(values), statistics.stdev(values), geomean(values), statistics.median(values))

    def plot_distribution(self, ax, alpha=1, label=None, bins=70, log=False):
        throughputs = self.get_throughputs_per_sec()
        ax.hist(throughputs, bins=bins, edgecolor='black', alpha=alpha, label=label, log=log)
        ax.set(xlabel='Moving Average Throughput (#RequestFinished/Sec)', ylabel='Frequency')


class DbproxyStatsDB(DB):
    def __init__(self, dbproxy_stats_csv_path):
        super(DbproxyStatsDB, self).__init__(csv_path=dbproxy_stats_csv_path)

    def get_num_dbproxies(self):
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
            res = perfdb.get_filtered(construct_filter(request_type, request_result)).get_latency_stats()
            if res is not None:
                mean, stddev, geomean, median = res

                request_type = 'All' if request_type is None else request_type
                request_result = 'All' if request_result is None else request_result
                print('Info:', '{:<27} {:<6} [Mean: {:>4.3f}] [SD: {:>4.3f}] [Geomean: {:>4.3f}] [Median: {:>4.3f}]'.format(request_type, request_result, mean, stddev, geomean, median))


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


def print_stats(perfdb, dbproxy_stats_db, run_name=None):
    print('Info: ==============================================================')
    print('Info:')
    print('Info:', 'Run:', str(run_name))
    print('Info:')

    # Apply filter on perfdb
    sr_perfdb = perfdb.get_filtered(successful_request_filter)

    sr_throughput = sr_perfdb.get_throughput()
    # sr_throughput.print_detailed_trajectory()
    # sr_throughput.print_trajectory()

    print('Info:')
    print('Info:', 'Successful Request:', len(sr_perfdb), '/', len(perfdb))
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
    print('Info:', 'num_dbproxy_db', dbproxy_stats_db.get_num_dbproxies())

    print('Info:')
    print_perfdb_latency_stats(perfdb)

    print('Info:')
    print_perfdb_success_ratio_stats(perfdb)

    print('Info:')
    dbproxy_stats_db.print_data()

    print('Info:')
    print('Info: ==============================================================')


def plot_distribution_charts(perfdb, dbproxy_stats_db, run_name=None):
    figsize = (16, 8)
    figname = 'distribution_' + str(run_name) + '_' + str(len(perfdb))
    fig = plt.figure(figname, figsize=figsize)
    fig.suptitle( 'Distribution with ' + str(perfdb.get_num_clients()) + ' Clients ' + str(dbproxy_stats_db.get_num_dbproxies()) + ' Dbproxies', fontsize=16)
    fig.set_tight_layout(True)

    axl = fig.add_subplot(1, 2, 1)
    axr = fig.add_subplot(1, 2, 2)

    # Category of interest
    request_types_oi = [['BeginTx', 'Commit', 'Rollback'], ['ReadOnly', 'ReadOnlyEarlyRelease'], ['WriteOnly', 'WriteOnlyEarlyRelease'], 'SingleReadOnly', 'SingleWriteOnly']

    for request_type in request_types_oi:
        request_type_str = request_type if type(request_type) == str else ' & '.join(sorted(set(map(lambda t: 'ReadOnly' if t == 'ReadOnlyEarlyRelease' else t, request_type))))

        # Get filtered
        filtered_perfdb = perfdb.get_filtered(construct_filter(request_type))

        if len(filtered_perfdb) == 0:
            continue

        # Plot throughput distribution
        filtered_throuput = filtered_perfdb.get_throughput()
        if filtered_throuput is not None:
            filtered_throuput.plot_distribution(axl, alpha=0.4, label=request_type_str, bins=50, log=False)

        # Plot latency distribution
        filtered_perfdb.plot_latency_distribution(axr, alpha=0.4, label=request_type_str, bins=50, log=True)

    axl.legend()
    axr.legend()

    plt.show()


def init(parser):
    parser.add_argument('log_dir', type=str, help='log file directory for single run')
    parser.add_argument('--nogui', action='store_true', help='Turn off gui')


def main(args):
    # Parse perf csv
    perfdb = PerfDB(perf_csv_path=os.path.join(args.log_dir, 'perf.csv.gz'))

    # Parse dbproxy stats csv
    dbproxy_stats_db = DbproxyStatsDB(os.path.join(args.log_dir, 'dbproxy_stats.csv.gz'))

    run_name = os.path.basename(os.path.dirname(os.path.join(args.log_dir, '')))

    # Print some stats
    print_stats(perfdb=perfdb, dbproxy_stats_db=dbproxy_stats_db, run_name=run_name)

    if not args.nogui:
        # Plot
        plot_distribution_charts(perfdb=perfdb, dbproxy_stats_db=dbproxy_stats_db, run_name=run_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    init(parser)
    main(parser.parse_args())
