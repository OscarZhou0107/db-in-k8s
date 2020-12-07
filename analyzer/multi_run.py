import argparse
import os
import time
import multiprocessing
import single_run


def float_fmt(num):
    return '{:.2f}'.format(num)


def parse_single_run_wrapper(single_arg):
    return parse_single_run(*single_arg)


def parse_single_run(run_name, args):
    '''
    (PerfDB, DbproxyStatsDB)
    '''
    if args.debug:
        print('Debug:', 'Parsing', run_name)

    perfdb = single_run.PerfDB(perf_csv_path=os.path.join(
        os.path.join(args.dir, run_name), 'perf.csv'))

    dbproxy_stats_db = single_run.DbproxyStatsDB(
        os.path.join(os.path.join(args.dir, run_name), 'dbproxy_stats.csv'))

    info_str = format('Info: Parsed {} with {} clients, {} dbproxies, {} unfiltered requests',
                      run_name, perfdb.get_num_clients(), dbproxy_stats_db.get_num_dbproxy(), len(perfdb))
    print(info_str)

    return (perfdb, dbproxy_stats_db)


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
    dataset = multiprocessing.Pool().map(parse_single_run_wrapper, map(
        lambda run_name: (run_name, args), run_names))
    end = time.time()
    print('Info:')
    print('Info:', 'Parsing took', float_fmt(end - start), 'seconds')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    init(parser)
    main(parser.parse_args())
