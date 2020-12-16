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
    parser.add_argument("--db_num", type=int)
    parser.add_argument("--mock_db", action='store_true', help="If use mock db")

    parser.add_argument("--python", type=str, default="python3", help="Python alias to use")

    args = parser.parse_args()
    username = args.username
    password = args.password
    python = args.python
    db_num = args.db_num

    mock_db = ""
    if args.mock_db:
        mock_db = "--mock_db"

    # TODO: add all machine # with a db server
    host_abbr = ["212", "244", "243", "242", "241"]
    host_num = len(host_abbr)
    # TODO: add db commands you want to execute on each machine -> should have a 1-1 mapping with # in host_abbr
    partial = "cargo run --release -- -c configug.toml --dbproxy "
    cmds = [partial + str(x) for x in range(host_num)]
    print(cmds)
    cmds = [cmd + mock_db for cmd in cmds]

    hosts = ["ug" + x + ".eecg.utoronto.ca" for x in host_abbr]
    print("host: {}".format(hosts[0]))

    # create all connections
    conns = []
    for i in range(db_num):
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
    for i in range(db_num):
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
    for i in range(db_num):
        if inout[i]:
            inout[i][0].write("cd /groups/qlhgrp/dv-in-rust/\r\n")
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
            for i in range(db_num):
                if inout[i]:
                    print("reading from stdout of {}".format(hosts[i]))
                    time.sleep(0.1)

                    # read whatever currently in the buffer
                    buffered = len(inout[i][1].channel.in_buffer)
                    #print("buffered {} bytes".format(buffered))
                    if buffered:
                        res = inout[i][1].read(buffered).decode("utf-8")
                        print(res.split("\n"))