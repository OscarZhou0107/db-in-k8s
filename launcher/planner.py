#!/usr/bin/python3

import argparse
import datetime
import itertools
import os
import random
import subprocess

import master


def get_ug_slaves():
    prefix = '128.100.13.'
    machines = itertools.chain(range(132, 181), range(201, 250))
    return list(map(lambda m: prefix + str(m), machines))


def check_alive(ip):
    cmds = ['ping', '-c', '1', ip]
    alive = False
    
    if ip is None:
        return alive
    
    try:
        subprocess.check_call(cmds, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        alive = True
    except subprocess.CalledProcessError:
        alive = False

    return alive


def single_run(args, client_num, client_mix, dbproxy_num):
    print('Info:')
    print('Info:', '=============================================================================================')
    print('Info:')
    print('Info:', 'Launching', 'client_num:', client_num, 'client_mix:', client_mix, 'dbproxy_num:', dbproxy_num)

    # Read in the conf
    conf = master.Conf(args.conf)
    
    # Find the addrs for dbproxies
    ug_slaves = itertools.cycle(get_ug_slaves())
    ug_candidates = list()
    for _ in range(dbproxy_num):
        ug_ip = None
        while not check_alive(ug_ip):
            ug_ip = next(ug_slaves)
        
        ug_port = random.randint(13000, 60000)  
        while str(ug_ip + ':' + str(ug_port)) in ug_candidates:
            ug_port = random.randint(13000, 60000)  

        ug_candidates.append(str(ug_ip + ':' + str(ug_port)))
    
    conf.clone_first_dbproxy(len(ug_candidates))
    for idx, ug_candidate in enumerate(ug_candidates):
        conf.set_dbproxy_addr(idx, ug_candidate)
    
    print('Info:', 'Assigned addresses to', len(ug_candidates), 'Dbproxies')
    if args.debug:
        print('debug:', ug_candidates)

    # Find the ports for scheduler, scheduler admin, and sequencer
    # Hardcode for now
    conf.update_scheduler_addr(new_port=2077)
    conf.update_scheduler_admin_addr(new_port=9999)
    conf.update_sequencer_addr(new_port=9876)
    conf.print_summary()
    # conf.update_scheduler_addr(new_port=random.randint(12000, 13000))
    # conf.update_scheduler_admin_addr(new_port=random.randint(11000, 12000))
    # conf.update_sequencer_addr(new_port=random.randint(10000, 11000))
    print('Info:', 'Assigned ports to Scheduler, Scheduler Admin and Sequencer')

    # Save the conf
    splitted = os.path.splitext(args.conf)
    args.new_conf = splitted[0] + '._ttmmpp_planner_' + splitted[1]
    conf.write(args.new_conf)

    # Run
    master_cmds = [args.python, os.path.join(args.remote_dv, 'launcher/master.py'), '--conf', args.new_conf, '--remote_dv', args.remote_dv, '--username', args.username, '--password', args.password, 
        '--duration', args.duration, '--client_num', client_num, '--client_mix', client_mix, '--perf_logging', args.perf_logging,
        '--python', args.python, '--output', args.output, '--bypass_stupid_check']
    master_cmds = list(map(lambda x: str(x), master_cmds))
    print('Info:', ' '.join(master_cmds))
    subprocess.Popen(master_cmds).wait()

    print('Info:', 'Done', 'client_num:', client_num, 'client_mix:', client_mix, 'dbproxy_num:', dbproxy_num)
    print('Info:')
    print('Info:', '=============================================================================================')
    print('Info:')


# python3 launcher/planner.py --conf=confug.toml --remote_dv=/groups/qlhgrp/liuli15/dv-in-rust --username= --password= --duration=60 --client_nums 100 200 --client_mixes 2 3 --dbproxy_nums 2 3
def main(args):
    total_num_tasks = len(args.client_nums) * len(args.client_mixes) * len(args.dbproxy_nums)
    print('Info:')
    print('Info:', 'client_nums:', args.client_nums)
    print('Info:', 'client_mixes:', args.client_mixes)
    print('Info:', 'dbproxy_nums:', args.dbproxy_nums)
    print('Info:', 'duration:', args.duration)
    print('Info:', 'perf_logging:', args.perf_logging)
    print('Info:')
    print('Info:', 'Total', total_num_tasks, 'tasks')
    if args.duration is not None:
        elapsed_est = datetime.timedelta(seconds=(total_num_tasks * args.duration))
        now = datetime.datetime.now()
        end_est = now + elapsed_est
        print('Info:', 'Now         :', now.strftime('%Y-%m-%d %H:%M:%S'))
        print('Info:', 'Est Finish  :', end_est.strftime('%Y-%m-%d %H:%M:%S'))
        print('Info:', 'Est Elapsed :', '{:.2f}'.format(elapsed_est.total_seconds()), 'seconds')
    print('Info:')

    prompt = ' '.join(['\n!!!!:', 'Is the setting correct? Run "cargo build --release" in', args.remote_dv, '?', '[y/n] > '])
    answer = input(prompt).lower()
    if answer != 'y':
        print('Error:', 'Go fix your stupid mistakes')
        exit()

    launch_time = datetime.datetime.now()
    # Run each sweep:
    for client_num in args.client_nums:
        for client_mix in args.client_mixes:
            for dbproxy_num in args.dbproxy_nums:
                single_run(args, client_num=client_num, client_mix=client_mix, dbproxy_num=dbproxy_num)
    finish_time = datetime.datetime.now()
    elapsed = finish_time - launch_time

    print('Info:')  
    print('Info:', '******************************************************')
    print('Info:', "ALL JOBS FINISHED")
    print('Info:', 'Launch      :', launch_time.strftime('%Y-%m-%d %H:%M:%S'))
    print('Info:', 'Finish      :', finish_time.strftime('%Y-%m-%d %H:%M:%S'))
    print('Info:', 'Elapsed     :', '{:.2f}'.format(elapsed.total_seconds()), 'seconds')
    print('Info:')
    print('Info:', 'Total       :', total_num_tasks, 'tasks')
    if args.duration is not None:
        elapsed_est = datetime.timedelta(seconds=(total_num_tasks * args.duration))
        end_est = launch_time + elapsed_est
        print('Info:')
        print('Info:', 'Est Finish  :', end_est.strftime('%Y-%m-%d %H:%M:%S'))
        print('Info:', 'Est Elapsed :', '{:.2f}'.format(elapsed_est.total_seconds()), 'seconds')
        if elapsed_est > elapsed:
            print('Warning:', 'Some jobs probably did not successfully finished')
    print('Info:', '******************************************************')
    print('Info:')


def init(parser):
    parser.description = '''
    Planner only looks conf for settings other than addrs and performance_logging.
    Scheduler and seqencer ips are determined on the fly, their ports are assigned randomly.
    Dbproxies are also created dynamically, with random addresses.
    Only the first dbproxy conf is used.
    '''
    # Required args
    parser.add_argument('--conf', type=str, required=True, help='Location of the conf in toml format')
    parser.add_argument('--remote_dv', type=str, required=True, help='Remote full absolute path for dv-in-rust directory')
    parser.add_argument('--username', type=str, required=True, help='Username for SSH')
    parser.add_argument('--password', type=str, required=True, help='Password for SSH')
    parser.add_argument('--client_nums', type=int, nargs='+', required=True, help='Number of clients to launch to sweep')
    parser.add_argument('--client_mixes', type=int, nargs='+', required=True, help='The workload mode for the client to sweep')
    parser.add_argument('--dbproxy_nums', type=int, nargs='+', required=True, help='Number of dbproxies to launch to sweep')

    # Optional args, important ones
    parser.add_argument('--duration', type=float, default=None, help='Time in seconds to auto terminate this script')
    parser.add_argument('--perf_logging', type=str, default='./perf', help='Dir to dump perf logging. Either absolute path, or relative path to --remote_dv!')

    # Optional args, not important
    parser.add_argument('--python', default='python3', help='Python to use (needs python3)')
    parser.add_argument('--output', type=str, default='./logging', help='Directory to forward the stdout and stderr of each subprocesses. Default is devnull. Either absolute path, or relative path to --remote_dv!')
    parser.add_argument('--debug', action='store_true', help='Trun on debug messages')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='master.py')
    init(parser)
    main(parser.parse_args())
