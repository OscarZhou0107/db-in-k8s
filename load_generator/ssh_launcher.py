import paramiko
import time
import os
import multiprocessing
import argparse
import math
import signal

DEBUG = 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--username", type=str, default="qinsinin")
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

    host_abbr = ["206", "207", "208", "209", "210"]
    hosts = ["ug" + x + ".eecg.utoronto.ca" for x in host_abbr]
    print("host: {}".format(hosts[0]))
    host_num = len(hosts)
    client_num_per_host = math.ceil(client_num/host_num)

    client_range_per_host = []
    num = 0
    for i in range(host_num - 1):
        num = num + client_num_per_host
        client_range_per_host.append("{} {}".format(num - client_num_per_host, num))
    client_range_per_host.append("{} {}".format(num, client_num))

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
            cmd = "{} {}launcher.py --mix {} --path {} --range {} {} {} --port {} --ip {}".format(python, args.path, mix, args.path, client_range_per_host[i], debug, mock_db, args.port, args.ip)
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
