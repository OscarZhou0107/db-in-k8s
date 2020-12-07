import argparse
import os
import time
import multiprocessing
import single_run
import datetime
from collections import defaultdict

try:
    import matplotlib.pyplot as plt
except:
    print('Error:', 'pip install matplotlib')


def float_fmt(num):
    return '{:.2f}'.format(num)


def parse_single_run_wrapper(single_arg):
    return parse_single_run(*single_arg)


def parse_single_run(run_name, args):
    '''
    (run_name, PerfDB, DbproxyStatsDB)
    '''
    if args.debug:
        print('Debug:', 'Parsing', run_name)

    perfdb = single_run.PerfDB(perf_csv_path=os.path.join(
        os.path.join(args.dir, run_name), 'perf.csv'))

    dbproxy_stats_db = single_run.DbproxyStatsDB(
        os.path.join(os.path.join(args.dir, run_name), 'dbproxy_stats.csv'))

    info_str = 'Info: Parsed {} with {} clients, {} dbproxies, {} unfiltered request datapoints'.format(
        run_name, perfdb.get_num_clients(), dbproxy_stats_db.get_num_dbproxy(), len(perfdb))
    print(info_str)

    return (run_name, perfdb, dbproxy_stats_db)


def plot_charts(database):
    '''
    [(run_name, PerfDB, DbproxyStatsDB)]
    '''
    # [(num_dbproxy, run_name, PerfDB)]
    database = list(
        map(lambda x: (x[2].get_num_dbproxy(), x[0], x[1]), database))
    # {num_dbproxy: [(run_name, PerfDB)]}
    database_by_num_dbproxy = defaultdict(list)
    for num_dbproxy, run_name, perfdb in database:
        database_by_num_dbproxy[num_dbproxy].append((run_name, perfdb))

    figsize = (16, 8)
    figname = 'scalability_' + \
        str(len(database)) + '_' + \
        datetime.datetime.now().strftime('%y%m%d_%H%M%S')
    fig = plt.figure(figname, figsize=figsize)
    fig.suptitle( 'Scalability of Throughput with varying Number of Clients and Dbproxies', fontsize=16)

    ax = fig.add_subplot()
    for num_dbproxy, dataset in database_by_num_dbproxy.items():
        print(num_dbproxy)

        def successful_query_filter(row):
            return row['request_result'] == 'Ok' and row['request_type'] in [
                'ReadOnly', 'WriteOnly', 'ReadOnlyEarlyRelease', 'WriteOnlyEarlyRelease']
        # [(run_name, num_clients, sq_perf_db, perfdb)]
        dataset = list(map(lambda x: (x[0], x[1].get_num_clients(), x[1].get_filtered(successful_query_filter), x[1]), dataset))

        # [(run_name, num_clients, max_sq_throughput, latency(mean, stddev, geomean, median), sq_perf_db, perfdb)]
        dataset = list(map(lambda x: (x[0], x[1], x[2].get_throughput().get_peak(), x[2].get_latency_stats(), x[2], x[3]), dataset))

        # Sort by num_clients
        dataset = sorted(dataset, key=lambda x: x[1])

        run_names, nums_clients, max_sq_throughputs, latencies = list(zip(*dataset))[0:4]
        _, max_thorughputs = tuple(zip(*max_sq_throughputs))
        
        print('nums_clients', nums_clients)
        print('max_throughputs', max_thorughputs)
        
        ax.plot(nums_clients, max_thorughputs, label=num_dbproxy, marker='o')  # , picker=True, pickradius=2)
        ax.set(xlabel='Number of Clients', ylabel='Peak Throughput on Successful Queries (#queries/sec)')
        ax.grid(axis='x', linestyle='--')
        ax.grid(axis='y', linestyle='-')
        ax.legend()

    plt.show()


def init(parser):
    parser.add_argument('--dir', type=str, required=True,
                        help='log files directory for all runs')
    parser.add_argument('--debug', action='store_true', help='debug messages')


def main(args):
    run_names = [o for o in os.listdir(
        args.dir) if os.path.isdir(os.path.join(args.dir, o))]
    print('Info:', 'Found metric data of', len(run_names), 'runs')

    print('Info:', 'Parsing in parallel...')
    start = time.time()
    database = multiprocessing.Pool().map(parse_single_run_wrapper, map(
        lambda run_name: (run_name, args), run_names))
    end = time.time()
    print('Info:')
    print('Info:', 'Parsing took', float_fmt(end - start), 'seconds')

    plot_charts(database)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    init(parser)
    main(parser.parse_args())
