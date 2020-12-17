#!/usr/bin/python3

import argparse
import cmd
import datetime
import os
import socket
import subprocess
import sys
import signal

try:
    import psutil
except:
    print('psutil is not installed. Try "pip install psutil"')


def sizeof_fmt(num, suffix='B'):
    '''
    https://stackoverflow.com/questions/1094841/reusable-library-to-get-human-readable-version-of-file-size
    '''
    for unit in ['','Ki']:
        if abs(num) < 1024.0:
            return '%3.2f%s%s' % (num, unit, suffix)
        num /= 1024.0
    return '%.2f%s%s' % (num, 'Mi', suffix)


def num_fmt(num):
    return f'{num:,}'


def float_fmt(num):
    return '{:.2f}'.format(num)


def print_load():
    print('Info:', socket.gethostname())
    print('Info:', psutil.cpu_count(logical=False), 'physical CPUs,', psutil.cpu_count(logical=True), 'logical CPUs,', '@', float_fmt(psutil.cpu_freq().current), 'MHz')
    print('Info:', str(psutil.cpu_percent()) + '%', 'CPU:', end=' ')
    print(*psutil.cpu_percent(percpu=True), sep='% ', end='%\n')

    if sys.platform.startswith('win'):
        load = psutil.getloadavg()
    else:
        load = os.getloadavg()
    print('Info:', 'Load:', *load)

    ram = psutil.virtual_memory()
    print('Info:', 'RAM:', sizeof_fmt(ram.used) + '/' + sizeof_fmt(ram.total), str(ram.percent) + '%')

    nio = psutil.net_io_counters()
    print('Info:', 'NET:', sizeof_fmt(nio.bytes_sent), 'Sent,', sizeof_fmt(nio.bytes_recv), 'Received,', num_fmt(nio.packets_sent), 'Packets Sent,', num_fmt(nio.packets_recv), 'Packets Received,', num_fmt(nio.errin), 'Error In,', num_fmt(nio.errout), 'Error Out')


class ControlPrompt(cmd.Cmd):
    def __init__(self, process_manager):
        '''
        process_manager is ProcessManager
        '''
        assert isinstance(process_manager, ProcessManager)
        super(ControlPrompt, self).__init__()
        self._process_manager = process_manager

    def do_list(self, arg=None):
        cur, prev = self._process_manager.list_process()
        diff = sorted(set(prev).difference(cur))
        diff_len = len(diff)
        if diff_len != 0:
            print('Info:', diff_len, 'processes are no longer running:')
            print('Info:', *diff)

        if len(cur) > 0:
            print('Info:', 'List of running processes:', len(cur))
            print('Info:', *cur)
        else:
            print('Info:', 'No running processes')
        
        if len(cur) == 0:
            print('Info:')
            print('Info:', 'All processes are done. Exiting')
            print('Info:')
            return True
        else:
            print('')

    def do_exit(self, arg=None):
        print('Warning:', 'Stopping running processes.. ALL')
        cur, _ = self._process_manager.list_process()

        for idx in cur:
            self._process_manager.stop_process(idx)
            print('Warning:', '    Stopped process', idx)
        
        return self.do_list()

    def do_stop(self, arg):
        request_to_stop = sorted(map(int, arg.split()))
        print('Info:', 'To Stop:', *request_to_stop)

        cur, _ = self._process_manager.list_process()

        to_stop = sorted(set(cur).intersection(request_to_stop))
        print('Warning:', 'Stopping running processes..', *to_stop)

        for idx in to_stop:
            self._process_manager.stop_process(idx)
            print('Warning:', '    Stopped process', idx)

        return self.do_list()

    def do_load(self, arg=None):
        print_load()


class ProcessManager:
    def __init__(self, process_creater):
        '''
        proc = process_creater(idx)
        '''
        self._process_creater = process_creater
        self._processes = list()

    def get_processes(self):
        return self._processes

    def launch_process(self):
        idx = len(self._processes)
        proc = self._process_creater(idx)
        assert isinstance(proc, subprocess.Popen)
        self._processes.append(proc)
        assert len(self._processes) == (idx+1)
        return idx

    def stop_process(self, idx):
        assert idx < len(self._processes)
        assert self._processes[idx] is not None
        os.kill(self._processes[idx].pid, signal.SIGINT)
        #self._processes[idx].terminate()
        self._processes[idx] = None

    def wait_process(self, idx):
        assert idx < len(self._processes)
        assert self._processes[idx] is not None
        self._processes[idx].wait()
        self._processes[idx] = None

    def wait_all(self):
        for idx in range(len(self._processes)):
            if self._processes[idx] is not None:
                self.wait_process(idx)

    def list_process(self):
        '''
        Update and return the list of processes still running
        (latest_running_process_idxs, previously_running_process_idxs)
        '''
        prev_idxs = sorted(map(lambda idx_proc: idx_proc[0], filter(lambda idx_proc: idx_proc[1] is not None, enumerate(self._processes))))
        
        def check_alive(proc):
            if proc is not None:
                if proc.poll() is None:
                    return proc
            else:
                return None
        self._processes = list(map(check_alive, self._processes))
        
        cur_idxs = sorted(map(lambda idx_proc: idx_proc[0], filter(lambda idx_proc: idx_proc[1] is not None, enumerate(self._processes))))

        return (cur_idxs, prev_idxs)

    def __del__(self):
        for proc in filter(None, self._processes):
            os.kill(proc.pid, signal.SIGINT)
            #proc.terminate()


# python launcher/slave.py --name=sequencer --cmd "cargo run -- --plain --sequencer" --output=./logging  --wd=./
def main(args):
    print('Info:', args)
    print('Info:')

    print('Info:')
    # cd into working directory
    os.chdir(args.wd)
    print('Info:', 'cd', args.wd)

    if args.sweeps is None or len(args.sweeps) == 0:
        args.sweeps = [None]
    
    if args.stdout: # stdout
        out_place_str = 'redirected into stdout'
    else:
        if args.output: # files
            if not os.path.exists(args.output):
                os.mkdir(args.output)
            dir_name = args.name.replace(' ', '') + '_' + datetime.datetime.now().strftime('%y%m%d_%H%M%S_%f')
            args.output = os.path.join(args.output, dir_name)
            if not os.path.exists(args.output):
                os.mkdir(args.output)
            out_place_str = 'redirected into ' + args.output
        else: # devnull
            out_place_str = 'supressed'

    print('Info:', 'All output of running processes are', out_place_str)

    # Launch jobs
    command = args.cmd.split()
    print('Info:')
    print('Info:', 'Launching', len(args.sweeps), 'processes "' + ' '.join(command) + '"')
    def launch_job(idx):
        print('Info:', '    Launching process', idx)
        if args.stdout: # stdout
            output = sys.stdout
        else:
            if args.output: # file
                if args.sweeps[idx] is None:
                    sweep_str = 'run'
                else:
                    sweep_str = str(args.sweeps[idx])
                output = open(os.path.join(args.output, sweep_str + '.log'), mode='w')
            else: # devnull
                output = subprocess.DEVNULL
        if args.sweeps[idx] is None:
            cmds = command
        else:
            cmds = command + [args.sweeps[idx]]
        return subprocess.Popen(cmds, stdout=output, stderr=output)

    pm = ProcessManager(launch_job)
    for _ in range(len(args.sweeps)):
        pm.launch_process()

    ControlPrompt(pm).cmdloop()


def init(parser):
    parser.add_argument('--name', type=str, default='slave', help='Description of the command')
    parser.add_argument('--wd', default='./', help='The working director for this command to run')
    parser.add_argument('--cmd', type=str, help='Command to launch (common part)')
    parser.add_argument('--sweeps', type=str, nargs='*', help='Argument (single word) to command to launch (diverging part)')

    parser.add_argument('--output', type=str, help='Directory to forward the stdout and stderr of each subprocesses. Default is devnull. Either absolute path, or relative path to --wd!')
    parser.add_argument('--stdout', action='store_true', help='Forward the stdout and stderr of each subprocesses to stdout. Default is devnull.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='slave.py')
    init(parser)
    main(parser.parse_args())
