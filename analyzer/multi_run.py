import argparse
import datetime
import multiprocessing
import os
import time
from collections import defaultdict

from analyzer import single_run

try:
    import matplotlib.pyplot as plt
except:
    print('Error:', 'pip install matplotlib')


__help__ = 'Parsing for multi run'


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

    perfdb = single_run.PerfDB(perf_csv_path=os.path.join(os.path.join(args.dir, run_name), 'perf.csv.gz'))

    dbproxy_stats_db = single_run.DbproxyStatsDB(os.path.join(os.path.join(args.dir, run_name), 'dbproxy_stats.csv.gz'))

    info_str = 'Info: Parsed {} with {} clients, {} dbproxies, {} unfiltered request datapoints'.format(run_name, perfdb.get_num_clients(), dbproxy_stats_db.get_num_dbproxy(), len(perfdb))
    print(info_str)

    return (run_name, perfdb, dbproxy_stats_db)


def plot_charts(args, database):
    '''
    [(run_name, PerfDB, DbproxyStatsDB)]
    '''
    # [(num_dbproxy, run_name, PerfDB)]
    database = list(map(lambda x: (x[2].get_num_dbproxy(), x[0], x[1]), database))
    # {num_dbproxy: [(run_name, PerfDB)]}
    database_by_num_dbproxy = defaultdict(list)
    for num_dbproxy, run_name, perfdb in database:
        database_by_num_dbproxy[num_dbproxy].append((run_name, perfdb))

    figsize = (16, 8)
    figname = 'scalability_' + str(len(database)) + '_' + datetime.datetime.now().strftime('%y%m%d_%H%M%S')
    fig = plt.figure(figname, figsize=figsize)
    fig.suptitle( 'Scalability of Throughput with varying Number of Clients and Dbproxies', fontsize=16)
    fig.set_tight_layout(True)

    axl = fig.add_subplot(1, 2, 1)
    axr = fig.add_subplot(1, 2, 2)
    for num_dbproxy, dataset in database_by_num_dbproxy.items():
        print(num_dbproxy)

        # [(run_name, num_clients, sr_perf_db, perfdb)]
        dataset = list(map(lambda x: (x[0], x[1].get_num_clients(), x[1].get_filtered(single_run.successful_request_filter), x[1]), dataset))

        # [(run_name, num_clients, max_sr_throughput, latency(mean, stddev, geomean, median), sr_perf_db, perfdb)]
        dataset = list(map(lambda x: (x[0], x[1], x[2].get_throughput().get_stats(), x[2].get_latency_stats(), x[2], x[3]), dataset))

        # Sort by num_clients
        dataset = sorted(dataset, key=lambda x: x[1])

        run_names, nums_clients, sr_throughputs_stats, latencies = list(zip(*dataset))[0:4]
        mean_throughputs, stddev_throughputs = tuple(zip(*sr_throughputs_stats))[1:3]
        mean_latencies, stddev_latencies = tuple(zip(*latencies))[0:2]
        
        print('nums_clients', nums_clients)
        print('mean_throughputs', mean_throughputs)
        print('stddev_throughputs', stddev_throughputs)
        print('mean_latencies', mean_latencies)
        print('stddev_latencies', stddev_latencies)
        
        # Left y-axis for throughput
        axl.errorbar(nums_clients, mean_throughputs, yerr=stddev_throughputs, label=str(num_dbproxy) + ' dbproxies', marker='s', capsize=3)  # , picker=True, pickradius=2)
        axl.set(xlabel='Number of Clients', ylabel='Average Throughput on Successful Queries (#queries/sec)')
        axl.grid(axis='x', linestyle='--')
        axl.grid(axis='y', linestyle='-')
        axl.legend()

        # Right y-axis for latency
        axr.errorbar(nums_clients, mean_latencies, yerr=stddev_latencies, label=str(num_dbproxy) + ' dbproxies', marker='s', capsize=3)
        axr.set(xlabel='Number of Clients', ylabel='Average Latency on Successful Queries (sec)')
        axr.grid(axis='x', linestyle='--')
        axr.grid(axis='y', linestyle='-')
        axr.legend()

    if args.output:
        filename = os.path.join(args.output, figname)
        plt.savefig(filename)
        print('Info:', 'Chart is dumped to', filename)

    plt.show()


def init(parser):
    parser.add_argument('dir', type=str, help='log files directory for all runs')
    parser.add_argument('--debug', action='store_true', help='debug messages')
    parser.add_argument('--output', type=str, help='directory to dump the files')


def main(args):
    if args.output is not None:
        print('Info', 'Preparing', args.output, 'for dumping files')
        if not os.path.exists(args.output):
            os.mkdirs(args.output)

    run_names = [o for o in os.listdir(args.dir) if os.path.isdir(os.path.join(args.dir, o))]
    print('Info:', 'Found metric data of', len(run_names), 'runs')

    print('Info:', 'Parsing in parallel...')
    start = time.time()
    database = multiprocessing.Pool().map(parse_single_run_wrapper, map(lambda run_name: (run_name, args), run_names))
    end = time.time()
    print('Info:')
    print('Info:', 'Parsing took', float_fmt(end - start), 'seconds')

    plot_charts(args, database)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    init(parser)
    main(parser.parse_args())
