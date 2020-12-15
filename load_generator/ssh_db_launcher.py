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

    # TODO: add all machine # with a db server
    host_abbr = ["212"]
    # TODO: add db commands you want to execute on each machine -> should have a 1-1 mapping with # in host_abbr
    cmds = ["cargo run -- --dbproxy 0"]

    hosts = ["ug" + x + ".eecg.utoronto.ca" for x in host_abbr]
    print("host: {}".format(hosts[0]))
    host_num = len(hosts)
    client_num_per_host = math.ceil(client_num/host_num)

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
    for i in range(host_num):
        if conns[i]:
            # get_pty means get a pseudo terminal. 
            # With it, if we close the ssh, the pty is also closed, 
            #     which sends a SIGHUP to cause the commands it ran to terminate
            # TODO: may need to pass in an actual command here rather than "", but it won't get executed anyway...
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
            for i in range(host_num):
                if inout[i]:
                    print("reading from stdout of {}".format(hosts[i]))
                    time.sleep(0.1)

                    # read whatever currently in the buffer
                    buffered = len(inout[i][1].channel.in_buffer)
                    #print("buffered {} bytes".format(buffered))
                    if buffered:
                        res = inout[i][1].read(buffered).decode("utf-8")
                        print(res.split("\n"))
