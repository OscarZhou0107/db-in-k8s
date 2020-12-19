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
    (run_name, (PerfDB, DbproxyStatsDB))
    '''
    if args.debug:
        print('Debug:', 'Parsing', run_name)

    perfdb = single_run.PerfDB(perf_csv_path=os.path.join(os.path.join(args.dir, run_name), 'perf.csv.gz'))

    dbproxy_stats_db = single_run.DbproxyStatsDB(os.path.join(os.path.join(args.dir, run_name), 'dbproxy_stats.csv.gz'))

    info_str = 'Info: Parsed {} with {} clients, {} dbproxies, {} unfiltered request datapoints'.format(run_name, perfdb.get_num_clients(), dbproxy_stats_db.get_num_dbproxies(), len(perfdb))
    print(info_str)

    return (run_name, (perfdb, dbproxy_stats_db))


def plot_charts(args, database):
    '''
    {run_name: (PerfDB, DbproxyStatsDB)}
    '''
    # [(num_dbproxy, run_name, PerfDB)]
    database_list = list(filter(lambda x: len(x[2])>0, map(lambda x: (x[1][1].get_num_dbproxies(), x[0], x[1][0]), database.items())))
    # {num_dbproxy: [(run_name, PerfDB)]}
    database_by_num_dbproxy = defaultdict(list)
    for num_dbproxy, run_name, perfdb in database_list:
        database_by_num_dbproxy[num_dbproxy].append((run_name, perfdb))

    figsize = (16, 8)
    figname = str(len(database_list)) + '_' + args.dir.replace('/', '_').replace('\\', '_').replace('.', '_') + '_' + datetime.datetime.now().strftime('%y%m%d_%H%M%S')
    fig = plt.figure(figname, figsize=figsize)
    fig.suptitle( 'Scalability of Throughput with varying Number of Clients and Dbproxies', fontsize=16)
    fig.set_tight_layout(True)

    axl = fig.add_subplot(1, 2, 1)
    axr = fig.add_subplot(1, 2, 2)

    artist_run_names = dict()
    for num_dbproxy, dataset in database_by_num_dbproxy.items():
        # [(run_name, num_clients, sr_perf_db, perfdb)]
        dataset = list(map(lambda x: (x[0], x[1].get_num_clients(), x[1].get_filtered(single_run.successful_request_filter), x[1]), dataset))

        # [(run_name, num_clients, sr_throughput(peak, mean, stddev, geomean, median), latency(mean, stddev, geomean, median), sr_perf_db, perfdb)]
        dataset = list(map(lambda x: (x[0], x[1], x[2].get_throughput().get_stats(), x[2].get_latency_stats(), x[2], x[3]), dataset))

        # Sort by num_clients
        dataset = sorted(dataset, key=lambda x: x[1])

        # Final data, all are linked by their index
        run_names, nums_clients, sr_throughputs_stats, latencies = tuple(zip(*dataset))[0:4]
        mean_throughputs = tuple(zip(*sr_throughputs_stats))[1:2]
        mean_latencies = tuple(zip(*latencies))[0]
        
        if args.debug:
            print('Debug:', 'nums_clients', nums_clients)
            print('Debug:', 'mean_throughputs', mean_throughputs)
            print('Debug:', 'mean_latencies', mean_latencies)
        
        # Left y-axis for throughput
        linel, = axl.plot(nums_clients, mean_throughputs, label=str(num_dbproxy) + ' dbproxies', marker='.', picker=True, pickradius=2)
        axl.set(xlabel='Number of Clients', ylabel='Average Throughput on Successful Queries (#queries/sec)')
        axl.grid(axis='x', linestyle='--')
        axl.grid(axis='y', linestyle='-')
        axl.legend()

        # Right y-axis for latency
        liner, = axr.plot(nums_clients, mean_latencies, label=str(num_dbproxy) + ' dbproxies', marker='.', picker=True, pickradius=2)
        axr.set(xlabel='Number of Clients', ylabel='Average Latency on Successful Queries (sec)')
        axr.grid(axis='x', linestyle='--')
        axr.grid(axis='y', linestyle='-')
        axr.legend()

        artist_run_names[linel] = run_names
        artist_run_names[liner] = run_names

    def on_pick(event):
        print('Info:')
        artist = event.artist
        if artist in artist_run_names:
            run_names = artist_run_names[artist]
            xmouse, ymouse = event.mouseevent.xdata, event.mouseevent.ydata
            x, y = artist.get_xdata(), artist.get_ydata()
            ind = event.ind
            indx = ind[0]
            print('Info:', 'Clicked: {:.2f}, {:.2f}'.format(xmouse, ymouse))
            print('Info:', 'Picked {} vertices: '.format(len(ind)), end='')
            if len(ind) != 1:
                print('Info:', 'Pick between vertices {} and {}'.format(min(ind), max(ind)+1))
            else:
                print('Info:', 'Picked vertice index:', indx)

            print('Info:', 'Selected: {:.2f}, {:.2f}'.format(x[indx], y[indx]))
            run_name = run_names[indx]
            single_run.print_stats(run_name=run_name, perfdb=database[run_name][0], dbproxy_stats_db=database[run_name][1])
            single_run.plot_distribution_charts(run_name=run_name, perfdb=database[run_name][0], dbproxy_stats_db=database[run_name][1])

    fig.canvas.callbacks.connect('pick_event', on_pick)

    if args.output:
        filename = os.path.join(args.output, figname)
        plt.savefig(filename)
        print('Info:', 'Chart is dumped to', filename)

    plt.show()


def check_data_validity(database):
    '''
    [(run_name, (PerfDB, DbproxyStatsDB))]

    return:
    [(run_name, (PerfDB, DbproxyStatsDB))]
    '''
    print('Info:')
    print('Info:', 'check_data_validity')
    # Main data structure to work with
    # [(size_perfdb, (run_name, (PerfDB, DbproxyStatsDB)))]
    original_size = len(database)
    datasize_list = list(map(lambda x: (len(x[1][0]), x), database))
    datasize_list.sort(key=lambda x: x[0])

    if len(datasize_list) == 0:
        return database

    filtered_database = list()
    for size, data in datasize_list:
        num_clients = data[1][0].get_num_clients()
        num_dbproxies = data[1][1].get_num_dbproxies()
        label = '(' + str(num_clients) + ' clients, ' + str(num_dbproxies) + ' dbproxies)'
        if size < 500:
            print('Critical:', data[0], label, 'only contains', size, 'datapoints. Excluded from analysis')
        else:
            if size < 2500:
                print('Warning:', data[0], label, 'only contains', size, 'datapoints')
            filtered_database.append(data)

    print('Critical:', original_size - len(filtered_database), '/', original_size, 'datasets do not have enough data. Excluded from analysis')
    return filtered_database


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
    database = check_data_validity(database)
    # {run_name: (PerfDB, DbproxyStatsDB)}
    database = dict(database)
    end = time.time()
    print('Info:')
    print('Info:', 'Parsing took', float_fmt(end - start), 'seconds')

    plot_charts(args, database)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    init(parser)
    main(parser.parse_args())
