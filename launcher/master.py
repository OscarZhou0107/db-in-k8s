#!/usr/bin/python3

import argparse
import cmd
import copy
import datetime
import itertools
import math
import multiprocessing
import os
import random
import signal
import socket
import subprocess
import telnetlib
import time
import warnings

import planner
import slave

warnings.filterwarnings(action='ignore',module='.*paramiko.*')

try:
    import paramiko
except:
    print('paramiko is not installed. Try "pip install paramiko"')

try:
    import toml
except:
    print('toml is not installed. Try "pip install toml"')


class SchedulerAdmin:
    def __init__(self, scheduler_admin_addr):
        iattempt = 0
        max_attempt = 30
        while iattempt < max_attempt:
            try:
                self._tn = telnetlib.Telnet(host=get_ip(scheduler_admin_addr), port=get_port(scheduler_admin_addr))
                print('Info:', 'Connected to Scheduler Admin at', scheduler_admin_addr)
                return
            except:
                print('Warning', 'Cannot connect to Scheduler Admin at', scheduler_admin_addr, 'Attempt', iattempt, '/', max_attempt)
                time.sleep(2)
                iattempt += 1
    
    def perf(self):
        print('Info:', 'Sending Perf request...')
        self._tn.write(b'perf\n')
        print('Info:', '...', self._tn.read_until(b'\n').decode('utf-8'))
        

class ControlPrompt(cmd.Cmd):
    def __init__(self, time, ssh_manager, conf, scheduler_admin):
        '''
        ssh_manager is SSHManager
        time = (launch_time, termination_time=None)
        conf is Conf
        '''
        assert isinstance(ssh_manager, SSHManager)
        assert isinstance(conf, Conf)
        assert isinstance(scheduler_admin, SchedulerAdmin)
        super(ControlPrompt, self).__init__()
        self._time = time
        self._ssh_manager = ssh_manager
        self._conf = conf
        self._scheduler_admin = scheduler_admin
    
    def get_time(self):
        return self._time
    
    def get_ssh_manager(self):
        return self._ssh_manager

    def get_conf(self):
        return self._conf

    def get_scheduler_admin(self):
        return self._scheduler_admin

    def do_perf(self, arg=None):
        try:
            self._scheduler_admin.perf()
        except:
            pass

    def do_list(self, arg=None):
        self._ssh_manager.refresh_ioe()
        num_machines = self._ssh_manager.get_num_machines()
        print('Info:', 'List of', num_machines, 'connected machines:')
        for idx in range(num_machines):
            print('Info:', '    ' + self._ssh_manager.get_machine_name_str(idx), ':', 'Running' if self._ssh_manager.get_ioe(idx) is not None else 'Idling')
        print('Info:')
        self.get_conf().print_summary()
        print('Info:')

    def do_talk(self, arg):
        '''
        Usage: talk idx {command}
        Info:
            1. if {command} is left empty, will simply refresh stdout
        '''
        arg = arg.split()
        if len(arg) < 1:
            print('Error:', 'Missing arguments')
            return

        idx = int(arg[0])
        forward_arg = None
        if len(arg) > 1:
            forward_arg = ' '.join(arg[1:])

        if not self.check_machine_existance(idx):
            return

        if forward_arg is not None:
            print('Info:', 'Forwarding', '"' + str(forward_arg) + '"', 'to', self._ssh_manager.get_machine_name_str(idx))

        # Get stdin, stdout, stderr
        ioe = self._ssh_manager.get_ioe(idx)
        if ioe is None:
            print('Warning:', 'Machine', idx, 'is not running any jobs')
            return
        else:
            i, o, e = ioe

        print('Info:')

        if forward_arg is not None:
            # Print stdout before forwarding to stdin
            o.channel.settimeout(1)
            try:
                for line in o:
                    print('        >', line.strip('\n'))
            except:
                pass
            
            # Print stderr before forwarding to stdin
            e.channel.settimeout(0.5)
            try:
                for line in e:
                    print('        >', line.strip('\n'))
            except:
                pass

            print('        $', forward_arg)
            # Forward to stdin
            i.write(forward_arg + '\n')
            i.flush()

        # Print stdout after forwarding to stdin
        o.channel.settimeout(1.5)
        try:
            for line in o:
                print('        >', line.strip('\n'))
        except:
            pass

        # Print stderr before forwarding to stdin
        e.channel.settimeout(0.5)
        try:
            for line in e:
                print('        >', line.strip('\n'))
        except:
            pass

        print('Info:')
        # Reset
        o.channel.settimeout(None)
    
    def do_time(self, arg=None):
        print_time(*self._time, True)
        print('Info:')

    def do_exit(self, arg=None):
        try:
            self._scheduler_admin.perf()
        except:
            pass
        print('Info:', 'Closing connections to', self._ssh_manager.get_num_machines(), 'machines')
        self._ssh_manager.close_all()
        print('Info: Done. Exiting')
        print('Info:')

        return True

    def check_machine_existance(self, idx):
        if idx >= self._ssh_manager.get_num_machines():
            print('Error:', idx, 'is not a valid Machine ID')
            return False
        return True


def print_time(launch_time, termination_time=None, show_elapsed=False):
    print('Info:', 'Launch      :', launch_time.strftime('%Y-%m-%d %H:%M:%S'))
    if show_elapsed or termination_time is not None:
        now = datetime.datetime.now()
        print('Info:', 'Now         :', now.strftime('%Y-%m-%d %H:%M:%S'))
    if show_elapsed:
        print('Info:', 'Elasped     :', '{:.2f}'.format((now - launch_time).total_seconds()), 'seconds')
    if termination_time is not None:
        print('Info:', 'Termination :', termination_time.strftime('%Y-%m-%d %H:%M:%S'))
        print('Info:', 'left        :', '{:.2f}'.format((termination_time - now).total_seconds()), 'seconds')
    

# Process tombstone endpoint 2
class SignalHandler():
    def __init__(self, ssh_manager, scheduler_admin):
        self._ssh_manager = ssh_manager
        self._scheduler_admin = scheduler_admin
        signal.signal(signal.SIGINT, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        try:
            self._scheduler_admin.perf()
        except:
            pass
        self._ssh_manager.__del__()
        exit(0)


class SSHManager:
    def __init__(self, machines, username, password):
        def connect_client(machine, username, password):
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            try:
                client.connect(machine, username=username, password=password)
                print('Info:', 'Connected to', machine, 'successfully')
                return client
            except:
                print('Error:', 'Could not connect to', machine)
                return None

        machines_connected = [(machine, connect_client(machine, username, password)) for machine in machines]
        machines_connected = list(filter(lambda x: x[1] is not None, machines_connected))
        
        self._machine_names = None
        self._machines = None
        self._ioe = None
    
        if len(machines_connected) > 0:
            self._machine_names, self._machines = zip(*machines_connected)
            self._ioe = [None] * len(self._machines)

    def get_num_machines(self):
        return len(self._machines)
    
    def get_machine(self, idx):
        assert idx < self.get_num_machines()
        return self._machines[idx]

    def get_ioe(self, idx):
        assert idx < self.get_num_machines()
        return self._ioe[idx]

    def refresh_ioe(self):
        def check_alive(ioe):
            if ioe is not None:
                if not ioe[2].channel.closed:
                    return ioe
            else:
                return None
        self._ioe = list(map(check_alive, self._ioe))

    def get_machine_name(self, idx):
        assert idx < self.get_num_machines()
        return self._machine_names[idx]

    def get_machine_name_str(self, idx):
        assert idx < self.get_num_machines()
        return '[' + str(idx) + ']' + ' ' + self._machine_names[idx]

    def launch_task_on_machine(self, idx, task_launcher):
        '''
        command = task_launcher(idx, machine, machine_name)
        '''
        assert idx < self.get_num_machines()
        assert task_launcher is not None
        self._ioe[idx] = self.get_machine(idx).exec_command('', get_pty=True)
        self._ioe[idx][0].write('{}\r\n'.format(task_launcher(idx, self.get_machine(idx), self.get_machine_name(idx))))
        self._ioe[idx][0].flush()

    def close_machine(self, idx):
        assert idx < self.get_num_machines()
        self._machines[idx].close()
        print('Info:', '    Closed', self.get_machine_name(idx))

    def close_all(self):
        if self._machines is not None:
            for idx in range(self.get_num_machines()):
                self.close_machine(idx)
    
            self._machine_names = None
            self._machines = None
            self._ioe = None
    
    def __del__(self):
        self.close_all()


def get_ip(addr):
    return addr.rpartition(':')[0]

def get_port(addr):
    return addr.rpartition(':')[2]


class Conf:
    def __init__(self, conf_path):
        self._conf_path = conf_path
        self._conf = toml.load(conf_path)

    def write(self, new_conf_path):
        print('Info:', 'Write updated conf to', new_conf_path)
        with open(new_conf_path, 'w') as f:
            toml.dump(self._conf, f)

    def get_performance_logging(self):
        return self._conf['scheduler'].get('performance_logging')

    def set_performance_logging(self, performance_logging):
        self._conf['scheduler']['performance_logging'] = performance_logging

    def clone_first_dbproxy(self, n):
        first_dbproxy = self._conf['dbproxy'][0]
        self._conf['dbproxy'] = [copy.deepcopy(first_dbproxy) for _ in range(n)]

    def set_dbproxy_addr(self, idx, addr):
        assert idx < len(self._conf['dbproxy'])
        self._conf['dbproxy'][idx]['addr'] = addr

    def get_all_dbproxy_addrs(self):
        return list(map(lambda c: c['addr'], self._conf['dbproxy']))

    def get_all_dbproxy_ips(self):
        return list(map(lambda addr: get_ip(addr), self.get_all_dbproxy_addrs()))

    def get_scheduler_addr(self):
        return self._conf['scheduler']['addr']

    def get_scheduler_ip(self):
        return get_ip(self.get_scheduler_addr())
    
    def get_scheduler_port(self):
        return get_port(self.get_scheduler_addr())

    def set_scheduler_addr(self, addr):
        self._conf['scheduler']['addr'] = addr
    
    def update_scheduler_addr(self, new_ip=None, new_port=None):
        prev_ip, separator, prev_port = self.get_scheduler_addr().rpartition(':')
        if new_ip is None:
            new_ip = prev_ip
        if new_port is None:
            new_port = prev_port
        self.set_scheduler_addr(str(new_ip) + separator + str(new_port))

    def get_scheduler_admin_addr(self):
        return self._conf['scheduler']['admin_addr']

    def get_scheduler_admin_ip(self):
        return get_ip(self.get_scheduler_admin_addr())

    def get_scheduler_admin_port(self):
        return get_port(self.get_scheduler_admin_addr())

    def set_scheduler_admin_addr(self, addr):
        self._conf['scheduler']['admin_addr'] = addr

    def update_scheduler_admin_addr(self, new_ip=None, new_port=None):
        prev_ip, separator, prev_port = self.get_scheduler_admin_addr().rpartition(':')
        if new_ip is None:
            new_ip = prev_ip
        if new_port is None:
            new_port = prev_port
        self.set_scheduler_admin_addr(str(new_ip) + separator + str(new_port))

    def get_sequencer_addr(self):
        return self._conf['sequencer']['addr']

    def get_sequencer_ip(self):
        return get_ip(self.get_sequencer_addr())

    def get_sequencer_port(self):
        return get_port(self.get_sequencer_addr())

    def set_sequencer_addr(self, addr):
        self._conf['sequencer']['addr'] = addr

    def update_sequencer_addr(self, new_ip=None, new_port=None):
        prev_ip, separator, prev_port = self.get_sequencer_addr().rpartition(':')
        if new_ip is None:
            new_ip = prev_ip
        if new_port is None:
            new_port = prev_port
        self.set_sequencer_addr(str(new_ip) + separator + str(new_port))

    def print_summary(self):
        print('Info:', 'Scheduler:', self.get_scheduler_addr())
        print('Info:', 'performance_logging:', self.get_performance_logging())
        print('Info:', 'Scheduler Admin:', self.get_scheduler_admin_addr())
        print('Info:', 'Sequencer:', self.get_sequencer_addr())
        print('Info:', 'Dbproxies:', self.get_all_dbproxy_addrs())


def prepare_conf(conf, args):
    cur_ip = socket.gethostbyname(socket.gethostname())
    print('Info:', 'Current IP:', cur_ip)
    
    # Existing Settings
    print('Info:')
    print('Info:', 'Existing Setting:')
    conf.print_summary()

    if args.follow_conf:
        args.new_conf = args.conf
        print('Info:', '--follow_conf. Will use the existing setting at', args.new_conf)
        return


    conf.set_performance_logging(args.perf_logging)
    # Set scheduler, scheduler_admin, and sequencer
    # to current machine using current machine's ip address,
    # instead of localhost. Ports are not modified
    conf.update_scheduler_addr(new_ip=cur_ip)
    conf.update_scheduler_admin_addr(new_ip=cur_ip)
    conf.update_sequencer_addr(new_ip=cur_ip)
    assert not planner.is_port_in_use(conf.get_scheduler_ip(), conf.get_scheduler_port())
    assert not planner.is_port_in_use(conf.get_scheduler_admin_ip(), conf.get_scheduler_admin_port())
    assert not planner.is_port_in_use(conf.get_sequencer_ip(), conf.get_sequencer_port())

    # New Settings
    print('Info:')
    print('Info:', 'New Setting:')
    conf.print_summary()

    # Write to file
    splitted = os.path.splitext(args.conf)
    args.new_conf = splitted[0] + '._ttmmpp_master_' + splitted[1]
    conf.write(args.new_conf)


def generate_cargo_run(which, conf_path, verbose=None, release=True):
    commands = ['cargo', 'run']
    if release:
        commands.append('--release')
    commands.append('--')
    commands.append(which)
    commands.extend(['-c', conf_path])
    commands.append('--plain')
    if verbose is not None:
        commands.append(verbose)
    return commands


def construct_cargo_launcher(args, machine_idx, verbose=None, release=True):
    # machines[0] == client_launcher
    # machines[1] == scheduler
    # machines[2] == sequencer
    # machines[3..] == dbproxies
    if machine_idx == 0:
        assert 'machine_idx should not use this launcher'
    elif machine_idx == 1:
        which = 'scheduler'
    elif machine_idx == 2:
        which = 'sequencer'
    else:
        which = 'dbproxy ' + str(machine_idx - 3)

    cargo_commands = generate_cargo_run(which='--' + which, conf_path=args.new_conf, verbose=verbose, release=release)
    cargo_command = '"' + ' '.join(cargo_commands) + '"'

    slave_path = os.path.join(args.remote_dv, 'launcher/slave.py')
    commands = [args.python, slave_path,'--name', '"' + which + '"', '--cmd', cargo_command, '--wd', args.remote_dv]
    if args.stdout:
        commands.append('--stdout')
    if args.output is not None:
        commands.extend(['--output', args.output])

    def launcher(idx, machine, machine_name):
        command = ' '.join(commands)
        print('Info:', 'Launching:')
        print('Info:', '    ' + '@', '[' + str(idx) + ']', machine_name)
        print('Info:', '    ' + command)
        return command
    return launcher


# python3 launcher/master.py --conf=confug.toml --remote_dv=/groups/qlhgrp/liuli15/dv-in-rust --username=xx --password=xx --duration=50
def main(args):
    print('Info:')

    # Prepare conf
    conf = Conf(args.conf)
    prepare_conf(conf, args)

    # Prepare ssh agents, will create a new ssh agent for every job
    print('Info:')
    print('Info:', 'Preparing SSH agents')
    scheduler = conf.get_scheduler_ip()
    print('Info:', 'Scheduler:', scheduler)
    scheduler_admin = conf.get_scheduler_admin_ip()
    print('Info:', 'Scheduler Admin:', scheduler_admin)
    sequencer = conf.get_sequencer_ip()
    print('Info:', 'Sequencer:', sequencer)
    dbproxies = conf.get_all_dbproxy_ips()
    print('Info:', 'Dbproxies:', dbproxies)
    client_launcher = scheduler
    print('Info:', 'Client Launcher:', client_launcher)
    # machines[0] == client_launcher
    # machines[1] == scheduler
    # machines[2] == sequencer
    # machines[3..] == dbproxies
    machines = [client_launcher, scheduler, sequencer] + dbproxies


    print('Info:')
    print('Info:', 'duration:', args.duration)
    print('Info:', 'perf_logging:', args.perf_logging)
    print('Info:', 'output', args.output)
    print('Info:')


    # Double check from stupid caller
    if args.bypass_stupid_check:
        print('Info:', 'Bypassing stupidness check')
    else:
        prompt = ' '.join(['\n!!!!:', 'Is the setting correct? Run "cargo build --release" in', args.remote_dv, '?', '[y/n] > '])
        answer = input(prompt).lower()
        if answer != 'y':
            print('Error:', 'Go fix your stupid mistakes')
            exit()

    # Launch ssh
    print('Info:')
    ssh_manager = SSHManager(machines=machines, username=args.username, password=args.password)
    print('Info:')
    # Launch cargos, cannot launch scheduler first!
    for machine_idx in reversed(range(2, ssh_manager.get_num_machines())):
        ssh_manager.launch_task_on_machine(machine_idx, construct_cargo_launcher(args=args, machine_idx=machine_idx, verbose=None, release=True))
    time.sleep(args.delay * ssh_manager.get_num_machines())
    # scheduler
    ssh_manager.launch_task_on_machine(1, construct_cargo_launcher(args=args, machine_idx=machine_idx, verbose=None, release=True))
    time.sleep(args.delay  * ssh_manager.get_num_machines() * 2)
    time.sleep(args.delay * 5)
    # Launch client launcher last
    def client_launcher_launcher(idx, machine, machine_name):
        assert idx == 0
        client_launcher_commands = [args.python, os.path.join(args.remote_dv, 'load_generator/ssh_launcher.py'), '--username', args.username,
            '--password', args.password, '--client_num', args.client_num, '--mix', args.client_mix,
            '--port', conf.get_scheduler_port(), '--ip', conf.get_scheduler_ip(), '--mock_db',
            '--path', os.path.join(args.remote_dv, 'load_generator')
            ]
        command = ' '.join(map(lambda x: str(x), client_launcher_commands))
        print('Info:', 'Launching:')
        print('Info:', '    ' + '@', '[' + str(idx) + ']', machine_name)
        print('Info:', '    ' + command)
        return command
    ssh_manager.launch_task_on_machine(0, client_launcher_launcher)

    print('Info:')
    print('Info:', 'Waiting for start up')
    time.sleep(args.delay * 10)

    # Launch scheduler admin
    print('Info:')
    scheduler_admin = SchedulerAdmin(conf.get_scheduler_admin_addr())

    # Register the signal handler
    sh = SignalHandler(ssh_manager, scheduler_admin)

    # Timer and auto terminator
    print('Info:') 
    launch_time = datetime.datetime.now()
    termination_time = None
    if args.duration is not None:
        print('Info:', 'Will terminate in', '{:.2f}'.format(args.duration), 'seconds')
        termination_time = datetime.datetime.now() + datetime.timedelta(seconds=args.duration)
        multiprocessing.Process(target=killer_process, args=(args.duration,), daemon=True).start()
    print_time(launch_time, termination_time)

    # Command loop
    print('Info:')
    ControlPrompt((launch_time, termination_time), ssh_manager, conf, scheduler_admin).cmdloop('DO NOT CTRL-C! Check <help>')
    sh.exit_gracefully(None, None)


def killer_process(wait_time):
    time.sleep(wait_time)
    print('')
    print('Info:', 'Terminate due to --duration!')
    os.kill(os.getppid(), signal.SIGINT)


def init(parser):
    parser.description = '''
    Launches and deploys all components according to the --conf configuration.
    By default, will launch Scheduler, Scheduler Admin and Sequencer to current machine
    using its public IP (rather than localhost or 127.0.0.1).
    '''
    # Required args
    parser.add_argument('--conf', type=str, required=True, help='Location of the conf in toml format')
    parser.add_argument('--remote_dv', type=str, required=True, help='Remote full absolute path for dv-in-rust directory')
    parser.add_argument('--username', type=str, required=True, help='Username for SSH')
    parser.add_argument('--password', type=str, required=True, help='Password for SSH')
    parser.add_argument('--client_num', type=int, required=True, help='Total number of clients to launch')
    parser.add_argument('--client_mix', type=int, required=True, help='The workload mode for the client')

    # Optional args, important ones
    parser.add_argument('--duration', type=float, default=None, help='Time in seconds to auto terminate this script')
    parser.add_argument('--perf_logging', type=str, default='./perf', help='Dir to dump perf logging. Either absolute path, or relative path to --remote_dv!')

    # Optional args, not important
    parser.add_argument('--python', default='python3', help='Python to use (needs python3)')
    parser.add_argument('--follow_conf', action='store_true', help='Follow the conf exactly')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay interval between jobs launching on each machine')
    parser.add_argument('--output', type=str, default='./logging', help='Directory to forward the stdout and stderr of each subprocesses. Default is devnull. Either absolute path, or relative path to --remote_dv!')
    parser.add_argument('--stdout', action='store_true', help='Forward the stdout and stderr of each subprocesses to stdout. Default is devnull.')
    parser.add_argument('--bypass_stupid_check', action='store_true', help='Bypass the stupidness check')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='master.py')
    init(parser)
    main(parser.parse_args())
