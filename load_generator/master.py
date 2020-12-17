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
import time
import warnings

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

# class SSHManager:
#     def __init__(self, machines, username, password):
#         def connect_client(machine, username, password):
#             client = paramiko.SSHClient()
#             client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#             try:
#                 client.connect(machine, username=username, password=password)
#                 print('Info:', 'Connected to', machine, 'successfully')
#                 return client
#             except:
#                 print('Error:', 'Could not connect to', machine)
#                 return None

#         machines_connected = [(machine, connect_client(machine, username, password)) for machine in machines]
#         machines_connected = list(filter(lambda x: x[1] is not None, machines_connected))
        
#         self.__machine_names = None
#         self.__machines = None
#         self.__ioe = None
    
#         if len(machines_connected) > 0:
#             self.__machine_names, self.__machines = zip(*machines_connected)
#             self.__ioe = [None] * len(self.__machines)

#     def get_num_machines(self):
#         return len(self.__machines)
    
#     def get_machine(self, idx):
#         assert idx < self.get_num_machines()
#         return self.__machines[idx]

#     def get_ioe(self, idx):
#         assert idx < self.get_num_machines()
#         return self.__ioe[idx]

#     def refresh_ioe(self):
#         def check_alive(ioe):
#             if ioe is not None:
#                 if not ioe[2].channel.closed:
#                     return ioe
#             else:
#                 return None
#         self.__ioe = list(map(check_alive, self.__ioe))

#     def get_machine_name(self, idx):
#         assert idx < self.get_num_machines()
#         return self.__machine_names[idx]

#     def get_machine_name_str(self, idx):
#         assert idx < self.get_num_machines()
#         return '[' + str(idx) + ']' + ' ' + self.__machine_names[idx]

#     def launch_task_on_machine(self, idx, task_launcher):
#         '''
#         (stdin, stdout, stderr) = task_launcher(idx, machine, machine_name)
#         '''
#         assert idx < self.get_num_machines()
#         assert task_launcher is not None
#         self.__ioe[idx] = task_launcher(idx, self.get_machine(idx), self.get_machine_name(idx))

#     def close_machine(self, idx):
#         assert idx < self.get_num_machines()
#         self.__machines[idx].close()
#         print('Info:', '    Closed', self.get_machine_name(idx))

#     def close_all(self):
#         if self.__machines is not None:
#             for idx in range(self.get_num_machines()):
#                 self.close_machine(idx)
    
#             self.__machine_names = None
#             self.__machines = None
#             self.__ioe = None
    
#     def __del__(self):
#         self.close_all()


# class ControlPrompt(cmd.Cmd):
#     def __init__(self, time, ssh_manager):
#         '''
#         ssh_manager is SSHManager
#         time = (launch_time, termination_time=None)
#         '''
#         assert isinstance(ssh_manager, SSHManager)
#         super(ControlPrompt, self).__init__()
#         self.__time = time
#         self.__ssh_manager = ssh_manager
    
#     def get_time(self):
#         return self.__time
    
#     def get_ssh_manager(self):
#         return self.__ssh_manager

#     def do_list(self, arg=None):
#         self.__ssh_manager.refresh_ioe()
#         num_machines = self.__ssh_manager.get_num_machines()
#         print('Info:', 'List of', num_machines, 'connected machines:')
#         for idx in range(num_machines):
#             print('Info:', '    ' + self.__ssh_manager.get_machine_name_str(idx), ':', 'Running' if self.__ssh_manager.get_ioe(idx) is not None else 'Idling')
#         print('')

#     def do_run(self, arg):
#         '''
#         Usage: run idx <command>
#         '''
#         arg = arg.split()
#         if len(arg) < 2:
#             print('Error:', 'Wrong number of arguments')
#             return

#         idx = int(arg[0])

#         if not self.check_machine_existance(idx):
#             return

#         if self.__ssh_manager.get_ioe(idx) is not None:
#             print('Error:', self.__ssh_manager.get_machine_name_str(idx), 'is already running')

#         def launcher(idx, machine, machine_name):
#             command = arg[1:]
#             command = list(map(lambda x: str(x), command))
#             command = ' '.join(command)
#             print('Info:', 'Launching:')
#             print('Info:', '    ' + '@', '[' + str(idx) + ']', machine_name)
#             print('Info:', '    ' + command)
#             return machine.exec_command(command, get_pty=True)
#         self.__ssh_manager.launch_task_on_machine(idx, launcher)
#         print('')

#     def do_talk(self, arg):
#         '''
#         Usage: talk idx {command}
#         Info:
#             1. if {command} is left empty, will simply refresh stdout
#         '''
#         arg = arg.split()
#         if len(arg) < 1:
#             print('Error:', 'Missing arguments')
#             return

#         idx = int(arg[0])
#         forward_arg = None
#         if len(arg) > 1:
#             forward_arg = ' '.join(arg[1:])

#         if not self.check_machine_existance(idx):
#             return

#         if forward_arg is not None:
#             print('Info:', 'Forwarding', '"' + str(forward_arg) + '"', 'to', self.__ssh_manager.get_machine_name_str(idx))

#         # Get stdin, stdout, stderr
#         ioe = self.__ssh_manager.get_ioe(idx)
#         if ioe is None:
#             print('Warning:', 'Machine', idx, 'is not running any jobs')
#             return
#         else:
#             i, o, e = ioe

#         print('Info:')

#         if forward_arg is not None:
#             # Print stdout before forwarding to stdin
#             o.channel.settimeout(1)
#             try:
#                 for line in o:
#                     print('        >', line.strip('\n'))
#             except:
#                 pass

#             print('        $', forward_arg)
#             # Forward to stdin
#             i.write(forward_arg + '\n')
#             i.flush()

#         # Print stdout after forwarding to stdin
#         o.channel.settimeout(1.5)
#         try:
#             for line in o:
#                 print('        >', line.strip('\n'))
#         except:
#             pass

#         print('Info:')
#         # Reset
#         o.channel.settimeout(None)
    
#     def do_time(self, arg=None):
#         print_time(*self.__time, True)
#         print('Info:')

#     def do_exit(self, arg=None):
#         print('Info:', 'Closing connections to', self.__ssh_manager.get_num_machines(), 'machines')
#         self.__ssh_manager.close_all()
#         print('Info: Done. Exiting')
#         print('Info:')

#         return True

#     def check_machine_existance(self, idx):
#         if idx >= self.__ssh_manager.get_num_machines():
#             print('Error:', idx, 'is not a valid Machine ID')
#             return False
#         return True


# class SuperClientControlPrompt(ControlPrompt):
#     def __init__(self, time, ssh_manager, args):
#         super(SuperClientControlPrompt, self).__init__(time, ssh_manager)
#         self.__args = args
    
#     def do_launch(self, arg):
#         '''
#         Usage: launch idx <count>
#         Info:
#             1. Will launch <count> number of processes to machine idx
#         ''' 
#         arg = arg.split()
#         if len(arg) != 2:
#             print('Error:', 'Wrong number of arguments')
#             return

#         idx = int(arg[0])
#         count = int(arg[1])
        
#         if not self.check_machine_existance(idx):
#             return

#         if self.get_ssh_manager().get_ioe(idx) is not None:
#             print('Error:', self.__ssh_manager.get_machine_name_str(idx), 'is already running')

#         self.get_ssh_manager().launch_task_on_machine(idx, construct_launcher(remote_launcher=self.__args.remote_launcher, cmd=self.__args.cmd, count=count, port=self.__args.port, stdout=self.__args.stdout))
#         print('')


# def construct_launcher(remote_launcher, cmd, count, port, stdout):
#     def launcher(idx, machine, machine_name):
#         command = [remote_launcher, '--cmd', cmd, '--count', count, '--port', port]
#         if stdout:
#             command.append('--stdout')
#         command = list(map(lambda x: str(x), command))
#         command = ' '.join(command)
#         print('Info:', 'Launching:')
#         print('Info:', '    ' + '@', '[' + str(idx) + ']', machine_name)
#         print('Info:', '    ' + command)
#         return machine.exec_command(command, get_pty=True)
#     return launcher


# def print_time(launch_time, termination_time=None, show_elapsed=False):
#     print('Info:', 'Launch      :', launch_time.strftime('%Y-%m-%d %H:%M:%S'))
#     if show_elapsed or termination_time is not None:
#         now = datetime.datetime.now()
#         print('Info:', 'Now         :', now.strftime('%Y-%m-%d %H:%M:%S'))
#     if show_elapsed:
#         print('Info:', 'Elasped     :', '{:.2f}'.format((now - launch_time).total_seconds()), 'seconds')
#     if termination_time is not None:
#         print('Info:', 'Termination :', termination_time.strftime('%Y-%m-%d %H:%M:%S'))
#         print('Info:', 'left        :', '{:.2f}'.format((termination_time - now).total_seconds()), 'seconds')


# def get_remote_machines(machines):
#     if machines is None:
#         machines = [
#             #'ug210.eecg.utoronto.ca', 
#             'ug211.eecg.utoronto.ca',
#             'ug212.eecg.utoronto.ca',
#             'ug213.eecg.utoronto.ca', 
#             'ug214.eecg.utoronto.ca', 
#             'ug215.eecg.utoronto.ca', 
#             'ug216.eecg.utoronto.ca', 
#             'ug217.eecg.utoronto.ca', 
#             'ug218.eecg.utoronto.ca', 
#             'ug219.eecg.utoronto.ca', 
#             'ug220.eecg.utoronto.ca', 
#             'ug221.eecg.utoronto.ca', 
#             'ug222.eecg.utoronto.ca', 
#             'ug223.eecg.utoronto.ca', 
#             'ug224.eecg.utoronto.ca',
#             'ug225.eecg.utoronto.ca',
#             'ug226.eecg.utoronto.ca',
#             'ug227.eecg.utoronto.ca',
#             'ug228.eecg.utoronto.ca',
#             'ug229.eecg.utoronto.ca',
#             'ug230.eecg.utoronto.ca',
#             'ug231.eecg.utoronto.ca',
#             'ug232.eecg.utoronto.ca',
#             'ug233.eecg.utoronto.ca',
#             'ug234.eecg.utoronto.ca',
#             'ug235.eecg.utoronto.ca',
#             'ug236.eecg.utoronto.ca',
#             'ug237.eecg.utoronto.ca',
#             'ug238.eecg.utoronto.ca',
#             'ug239.eecg.utoronto.ca',
#             'ug240.eecg.utoronto.ca'
#             ]
#         random.shuffle(machines)
#     return machines


# def launch_tasks(sshmanager, total_count, remote_launcher, remote_cmd, port, delay, stdout=False, is_unevenly=False, threshold=1000):
#     print('Info:')
#     machine_iter = itertools.cycle(range(sshmanager.get_num_machines()))
#     count_left = total_count
    
#     if is_unevenly:
#         target_count_to_use = threshold
#         print('Info:', 'Schedule to run', target_count_to_use, 'jobs to each of the', math.ceil(count_left / target_count_to_use), 'machines')
#     else:
#         target_count_to_use = math.ceil(count_left / sshmanager.get_num_machines())
#         print('Info:', 'Schedule to run', target_count_to_use, 'jobs on every machine')

#     while count_left > 0:
#         machine_idx_to_run = next(machine_iter)
#         count_to_use = min(target_count_to_use, count_left)

#         sshmanager.launch_task_on_machine(machine_idx_to_run, construct_launcher(remote_launcher=remote_launcher, cmd=remote_cmd, count=count_to_use, port=port, stdout=stdout))
#         time.sleep(delay)

#         count_left = count_left - count_to_use


# def main(args):
#     print('Info:', args)
#     print('Info:')

#     args.machines = get_remote_machines(args.machines)

#     if args.admin:
#         print('Info:', 'Running in admin mode')
#     else:
#         print('Info:', 'Runing a total of', args.count, 'processes')
#         required_machines_count = (int((args.count - 1) / args.threshold) + 1)
#         if required_machines_count > len(args.machines):
#             print('Error:', 'Not enough machines for running', args.count, 'jobs')
#             print('Info:', '    Current computing power is', args.threshold, '*', len(args.machines), '=', args.threshold * len(args.machines))
#             print('Info:', '    Still needs', int(required_machines_count - len(args.machines)), 'machines')
#             exit(0)

#     sm = SSHManager(args.machines, args.username, args.password)
#     print('Info:')
#     launch_time = datetime.datetime.now()
#     print_time(launch_time)

#     if not args.admin:
#         launch_tasks(sshmanager=sm, total_count=args.count, remote_launcher=args.remote_launcher, remote_cmd=args.cmd, port=args.port, delay=args.delay, stdout=args.stdout, is_unevenly=args.unevenly, threshold=args.threshold)
    
#     print('Info:')
#     termination_time = None
#     if args.duration is not None:
#         print('Info:', 'Will terminate in', '{:.2f}'.format(args.duration), 'seconds')
#         termination_time = datetime.datetime.now() + datetime.timedelta(seconds=args.duration)
#         print_time(launch_time, termination_time)
#         multiprocessing.Process(target=killer_process, args=(args.duration,), daemon=True).start()
    
#     SuperClientControlPrompt((launch_time, termination_time), sm, args).cmdloop()


# def killer_process(wait_time):
#     time.sleep(wait_time)
#     print('')
#     print('Info:', 'Terminate!')
#     os.kill(os.getppid(), signal.SIGTERM)


# def parse_arguments():
#     parser = argparse.ArgumentParser(description='super_client.py')
#     parser.add_argument('--admin', action='store_true', help='SSH to all the machines, but without executing any commands')
#     parser.add_argument('--remote_launcher', type=str, default='~/ece1747/SimMud/run_client.py', help='Location of remoate_launcher in remote location, aka, run_client.py')
#     parser.add_argument('--count', type=int, default=1000, help='Number of processes to deploy')
#     parser.add_argument('--unevenly', action='store_true', help='Stack --threshold jobs on the same machine')
#     parser.add_argument('--threshold', type=int, default=500, help='Limited number of processes to launch for each machine')
#     parser.add_argument('--delay', type=float, default=1.0, help='Delay interval between jobs launching on each machine')
#     parser.add_argument('--duration', type=float, default=None, help='Time in seconds to auto terminate this script')
#     # Forwarded to remote_launcher
#     parser.add_argument('--port', type=str, default=':1747', help='Forward to remote_launcher Server @<IP>:<PORT>')
#     parser.add_argument('--cmd', type=str, default='~/ece1747/SimMud/client', help='Forward to remote_launcher --cmd')
#     parser.add_argument('--stdout', action='store_true', help='Forward to remote_launcher --stdout')
#     # SSH-related
#     parser.add_argument('--machines', type=str, nargs='+', help='Pool of machines for SSH')
#     parser.add_argument('--username', type=str, required=True, help='Username for SSH')
#     parser.add_argument('--password', type=str, required=True, help='Password for SSH')
    
#     return parser.parse_args()


# if __name__ == '__main__':
#     main(parse_arguments())
    

# Process tombstone endpoint 1
# class SuperControlPrompt(super_client.ControlPrompt):
#     def __init__(self, time, ssh_manager, label_message):
#         super(SuperControlPrompt, self).__init__(time, ssh_manager)
#         self.__label_message = label_message

#     def do_load(self, arg=None):
#         run_client.print_load()

#     def do_label(self, arg=None):
#         print('Info:', self.__label_message.get_label())
#         print('Info:')


# class ServerProcessManager(run_client.ProcessManager):
#     def __init__(self, process_creater):
#         super(ServerProcessManager, self).__init__(process_creater)

#     def stop_process(self, idx):
#         assert idx < len(self.get_processes())
#         assert self.get_processes()[idx] is not None
#         print('Info:', '    Stopping server:', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
#         outs, errs = self.get_processes()[idx].communicate(input=bytes('q\n', 'ascii'))
#         print('Info:', '    Stopped server:', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
#         if outs:
#             for line in outs.decode('utf-8').splitlines():
#                 print('Server STDOUT:', '    ', line)
#         if errs:
#             for line in errs.decode('utf-8').splitlines():
#                 print('Server STDERR:', '    ', line)
#         self.get_processes()[idx] = None

#     def __del__(self):
#         for idx in filter(lambda idx: self.get_processes()[idx] is not None, range(len(self.get_processes()))):
#             self.stop_process(idx)


# class LabelMessenger():
#     def __init__(self, quest_noquest, spread_static, count):
#         self.__quest_noquest = quest_noquest
#         self.__spread_static = spread_static
#         self.__count = count
    
#     def get_label(self):
#         return (self.__quest_noquest, self.__spread_static, str(self.__count))

#     def print_git_message(self):
#         print('Info:')
#         print('Info:', 'Don\'t forget to git!')
#         print('Info:', '    ', 'git status')
#         print('Info:', '    ', 'git add .')
#         print('Info:', '    ', 'git commit -m \'' + ' '.join(self.get_label()) + '\'')
#         print('Info:', '    ', 'git pull')
#         print('Info:', '    ', 'git push')
#         print('Info:')

#     def __del__(self):
#         self.print_git_message()


# # Process tombstone endpoint 2
# class SignalHandler():
#     def __init__(self, ssh_manager, server_process_manager, label_message):
#         self.__ssh_manager = ssh_manager
#         self.__server_process_manager = server_process_manager
#         self.__label_message = label_message
#         signal.signal(signal.SIGTERM, self.exit_gracefully)

#     def exit_gracefully(self, signum, frame):
#         self.__ssh_manager.__del__()
#         self.__server_process_manager.__del__()
#         exit(0)


# def get_server_config(path, quest, noquest, spread, static):
#     config_path = None
#     if quest:
#         if spread:
#             config_path = os.path.join(path, 'config_spread_quest.ini')
#         else:
#             assert static
#             config_path = os.path.join(path, 'config_static_quest.ini')
#     else:
#         assert noquest
#         if spread:
#             config_path = os.path.join(path, 'config_spread_no_quest.ini')
#         else:
#             assert static
#             config_path = os.path.join(path, 'config_static_no_quest.ini')
    
#     if os.path.isfile(config_path):
#         return config_path
#     else:
#         return None


# allowed_server_host = [
#     'ug205', 'ug206', 'ug207', 'ug208', 'ug209',
#     'ug178', 'ug177', 'ug176', 'ug175', 'ug174',
#     'ug173', 'ug172', 'ug171', 'ug170', 'ug169',
#     'ug168', 'ug167', 'ug166', 'ug165', 'ug164',
#     'ug163']


# def main(args):
#     # print('Info:', args)
#     print('Info:')

#     cur_host_name = socket.gethostname()
#     print('Info:', '@' + cur_host_name)
#     if not args.disable_server_check:
#         if cur_host_name not in allowed_server_host:
#             print('Error:', 'Current server host', '@' + cur_host_name, 'is not allowed')
#             print('Error:', '    ', 'List of allowed server hosts:', allowed_server_host)
#             exit(0)

#     local_path = os.path.expanduser(args.path)
#     config_path = get_server_config(path=local_path, quest=args.quest, noquest=args.noquest, spread=args.spread, static=args.static)
#     if config_path is None:
#         print('Error:', 'Could not find server config file in', local_path)
#         exit(0)

#     args.client_machines = super_client.get_remote_machines(args.client_machines)

#     sm = super_client.SSHManager(args.client_machines, args.username, args.password)
#     if sm.get_num_machines() == 0:
#         print('Error:', 'Could not connect to any of the client machines!')
#         exit(0)
#     print('Info:')

#     if args.port is None:
#         args.port = random.randint(1500, 60000)

#     server_host_port = cur_host_name + ':' + str(args.port)
#     def server_launcher(_):
#         print('Info:', 'Launching server process', '@' + server_host_port)
#         cmd = [os.path.join(local_path, 'server'), config_path, str(args.port)]
#         print('Info:', '    ', ' '.join(cmd))
#         return subprocess.Popen(cmd, stdin=subprocess.PIPE)#, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#     spm = ServerProcessManager(server_launcher)

#     # Auto messenger on exit
#     label_msger = LabelMessenger('quest' if args.quest else 'noquest', 'spread' if args.spread else 'static', args.count)

#     # Register the signal handler
#     sh = SignalHandler(sm, spm, label_msger)

#     spm.launch_process()
#     time.sleep(5 * args.delay)

#     print('Info:')
#     launch_time = datetime.datetime.now()
#     super_client.print_time(launch_time)

#     super_client.launch_tasks(
#         sshmanager=sm, 
#         total_count=args.count, 
#         remote_launcher=os.path.join(args.path, 'run_client.py'), 
#         remote_cmd=os.path.join(args.path, 'client'), 
#         port=server_host_port, 
#         delay=args.delay)
    
#     print('Info:')
#     termination_time = None
#     if args.duration is not None:
#         print('Info:', 'Will terminate in', '{:.2f}'.format(args.duration), 'seconds')
#         termination_time = datetime.datetime.now() + datetime.timedelta(seconds=args.duration)
#         super_client.print_time(launch_time, termination_time)
#         multiprocessing.Process(target=killer_process, args=(args.duration,), daemon=True).start()
    
#     print('Info:')
#     SuperControlPrompt((launch_time, termination_time), sm, label_msger).cmdloop('DO NOT CTRL-C!')
#     sh.exit_gracefully(None, None)


# def killer_process(wait_time):
#     time.sleep(wait_time)
#     print('')
#     print('Info:', 'Terminate due to --duration!')
#     os.kill(os.getppid(), signal.SIGTERM)


# def parse_arguments():
#     parser = argparse.ArgumentParser(description='super.py')
#     # Optional
#     parser.add_argument('--path', type=str, default='~/ece1747/SimMud', help='Directory')
#     parser.add_argument('--delay', type=float, default=1.0, help='Delay interval between jobs launching on each machine')
#     parser.add_argument('--duration', type=float, default=None, help='Time in seconds to auto terminate this script')
#     parser.add_argument('--port', type=int, default=None, help='Port to use. Random by default')
#     parser.add_argument('--client_machines', type=str, nargs='+', help='Pool of machines for client')
#     parser.add_argument('--disable_server_check', action='store_true', help='Disable the server machine check')
#     # Required
#     parser.add_argument('--username', type=str, required=True, help='Username for SSH')
#     parser.add_argument('--password', type=str, required=True, help='Password for SSH')
#     qmode_group = parser.add_mutually_exclusive_group(required=True)
#     qmode_group.add_argument('--quest', action='store_true')
#     qmode_group.add_argument('--noquest', action='store_true')
#     lmode_group = parser.add_mutually_exclusive_group(required=True)
#     lmode_group.add_argument('--static', action='store_true')
#     lmode_group.add_argument('--spread', action='store_true')
#     parser.add_argument('--count', type=int, required=True, help='Number of clients to deploy')
    
#     return parser.parse_args()

class Conf(dict):
    def __init__(self, conf_path):
        self._conf_path = conf_path
        self._conf = toml.load(conf_path)

    def get_all_dbproxy_addrs(self):
        return list(map(lambda c: c['addr'], self._conf['dbproxy']))

    def get_scheduler_addr_port(self):
        return self._conf['scheduler']['addr']

    def set_scheduler_addr_port(self, addr):
        self._conf['scheduler']['addr'] = addr
    
    def update_scheduler_addr(self, new_ip):
        _prev_ip, separator, port = self.get_scheduler_addr_port().rpartition(':')
        self.set_scheduler_addr_port(new_ip + separator + port)

    def get_scheduler_admin_addr(self):
        return self._conf['scheduler']['admin_addr']

    def set_scheduler_admin_addr(self, addr):
        self._conf['scheduler']['admin_addr'] = addr

    def update_scheduler_admin_addr(self, new_ip):
        _prev_ip, separator, port = self.get_scheduler_admin_addr().rpartition(':')
        self.set_scheduler_admin_addr(new_ip + separator + port)

    def get_sequencer_addr(self):
        return self._conf['sequencer']['addr']

    def set_sequencer_addr(self, addr):
        self._conf['sequencer']['addr'] = addr

    def update_sequencer_addr(self, new_ip):
        _prev_ip, separator, port = self.get_sequencer_addr().rpartition(':')
        self.set_sequencer_addr(new_ip + separator + port)

    def print_addrs(self):
        print('Info:', 'Addrs Settings:')
        scheduler = self.get_scheduler_addr_port()
        print('Info:', 'Scheduler:', scheduler)
        scheduler_admin = self.get_scheduler_admin_addr()
        print('Info:', 'Scheduler Admin:', scheduler_admin)
        sequencer = self.get_sequencer_addr()
        print('Info:', 'Sequencer:', sequencer)
        dbproxies = self.get_all_dbproxy_addrs()
        print('Info:', 'Dbproxies:', dbproxies)


def prepare_conf(conf, args):
    cur_ip = socket.gethostbyname(socket.gethostname())
    print('Info:', 'Current IP:', cur_ip)
    
    # Existing Settings
    print('Info:')
    print('Info:', 'Existing Setting:')
    conf.print_addrs()

    if args.follow_conf:
        print('Info:', '--follow_conf. Will use the existing setting!')
        return

    # Set scheduler, scheduler_admin, and sequencer
    # to current machine using current machine's ip address,
    # instead of localhost. Ports are not modified
    conf.update_scheduler_addr(cur_ip)
    conf.update_scheduler_admin_addr(cur_ip)
    conf.update_sequencer_addr(cur_ip)
    # New Settings
    print('Info:')
    print('Info:', 'New Setting:')
    conf.print_addrs()


def main(args):
    print('Info:')

    cur_ip = socket.gethostbyname(socket.gethostname())
    print('Info:', 'Current IP:', cur_ip)

    conf = Conf(args.conf)
    prepare_conf(conf, args)


def init(parser):
    parser.description = '''
    Launches and deploys all components according to the --conf configuration.
    By default, will launch Scheduler, Scheduler Admin and Sequencer to current machine
    using its public IP (rather than localhost or 127.0.0.1).
    '''
    parser.add_argument('--conf', type=str, required=True, help='Location of the conf in toml format')
    parser.add_argument('--follow_conf', action='store_true', help='Follow the conf exactly')

    parser.add_argument('--output', type=str, help='Directory to forward the stdout and stderr of each subprocesses. Default is devnull. Be aware of concurrent file writing!')
    parser.add_argument('--stdout', action='store_true', help='Forward the stdout and stderr of each subprocesses to stdout. Default is devnull.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='master.py')
    init(parser)
    main(parser.parse_args())
