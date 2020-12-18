import paramiko
import time
import os
import multiprocessing
import argparse
import math
import signal
import random

DEBUG = 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--username", type=str)
    parser.add_argument("--password", type=str)
    parser.add_argument("--client_num", type=int, required=True)
    parser.add_argument("--port", type=int, default=2077)
    parser.add_argument("--ip", type=str, default='128.100.13.240')
    parser.add_argument("--path", type=str, default='/groups/qlhgrp/dv-in-rust/load_generator/')

    parser.add_argument("--mix", type=int, required=True)
    parser.add_argument("--python", type=str, default="python3", help="Python alias to use")
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--mock_db", action='store_true')

    args = parser.parse_args()
    username = args.username
    password = args.password
    client_num = args.client_num
    mix = args.mix
    python = args.python
    debug = ""
    if args.debug:
        debug = "--debug"
    mock_db = ""
    if args.mock_db:
        mock_db = "--mock_db"

    host_abbr = ['132', '133', '134', '135', '136', '137', '138', '139', '140', '141', '142', '143', '144', '145', '146', '147', '148', '149', '150', '151', '152', '153', '154', '155', '156', '157', '158', '159', '160', '161', '162', '163', '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175', '176', '177', '178', '179', '180', '201', '202', '203', '204', '205', '206', '207', '208', '209', '210', '211', '212', '213', '214', '215', '216', '217', '218', '219', '220', '221', '222', '223', '224', '225', '226', '227', '228', '229', '230', '231', '232', '233', '234', '235', '236', '237', '238', '239', '240', '241', '242', '243', '244', '245', '246', '247', '248', '249']
    random.shuffle(host_abbr)
    hosts = ["ug" + x + ".eecg.utoronto.ca" for x in host_abbr]
    print("host: {}".format(hosts[0]))
    host_num = len(hosts)
    client_num_per_host = math.ceil(client_num/host_num)

    client_range_per_host = []
    num = 0
    for i in range(host_num):
        num = num + client_num_per_host
        if num >= client_num:
            client_range_per_host.append("{} {}".format(num - client_num_per_host, client_num))
            break
        client_range_per_host.append("{} {}".format(num - client_num_per_host, num))

    host_num = len(client_num_per_host)

    #host_index = [str(x) for x in range(host_num)]
    
    # create all connections
    conns = []
    for i in range(host_num):
        conn = paramiko.SSHClient()
        conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            conn.connect(hosts[i], username=username, password=password)
        except:
            print("connection to {} failed".format(hosts[i]))
            conns.append(None)
            continue
        conns.append(conn)
    
    # start shells and get stdin, stdout, stderr
    inout = []
    cmds = []
    for i in range(host_num):
        if conns[i]:
            cmd = "{} {} --mix {} --path {} --range {} {} {} --port {} --ip {}".format(python, 
                os.path.join(args.path, 'launcher.py'), mix, args.path, client_range_per_host[i], debug, mock_db, args.port, args.ip)
            cmds.append(cmd)
            print(cmd)
            #if DEBUG: 
               #cmd = "python3 ssh_test.py --range {}".format(client_range_per_host[i])
               #cmd = "pwd"
            # get_pty means get a pseudo terminal. 
            # With it, if we close the ssh, the pty is also closed, 
            #     which sends a SIGHUP to cause the commands it ran to terminate
            stdin, stdout, stderr = conns[i].exec_command("", get_pty=True)
            inout.append([stdin, stdout, stderr])
        else:
            inout.append(None)

    # otherwise read/write to std stream might fail
    time.sleep(5)

    # run all cmds
    for i in range(host_num):
        if inout[i]:
            inout[i][0].write("{}\r\n".format(cmds[i]))
            inout[i][0].flush()
            print("reading from stdout of {}".format(hosts[i]))
            time.sleep(0.1)

            # read whatever currently in the buffer
            buffered = len(inout[i][1].channel.in_buffer)
            #print("buffered {} bytes".format(buffered))
            if buffered:
                res = inout[i][1].read(buffered).decode("utf-8")
                print(res.split("\n"))

    print("starting while loop...")
    while True:
        text = input("kill or status: \n")
        if text == "kill":
            os.killpg(os.getpid(), signal.SIGTERM)
        if text == "status":
        #else:
            for i in range(host_num):
                if inout[i]:
                    inout[i][0].write("{}\r\n".format(text))
                    inout[i][0].flush()
                    print("reading from stdout of {}".format(hosts[i]))
                    time.sleep(0.1)

                    # read whatever currently in the buffer
                    buffered = len(inout[i][1].channel.in_buffer)
                    #print("buffered {} bytes".format(buffered))
                    if buffered:
                        res = inout[i][1].read(buffered).decode("utf-8")
                        print(res.split("\n"))
