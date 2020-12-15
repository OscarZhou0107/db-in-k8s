import sys
import subprocess
import os
import time
import argparse
import os
import signal
import multiprocessing

DEBUG = 0

def launch_client(cids, mix, pid, python, debug, mock_db, ssh):
    script = "client.py"
    if DEBUG:
        script = "test.py"
    if ssh:
        script = "/groups/qlhgrp/dv-in-rust/load_generator/client.py"

    procs = {}
    port = 2077

    print("Popen child processes...")

    # With subprocess.Popen(), all child processes terminates if the parent is terminated with ctrl-C
    # However, if parent has a bug/ throws exception, child processes continue running
    # -> handle this with finally
    for cid in cids:
        if DEBUG:
            command = "{} {} --c_id {}".format(python, script, cid)
            procs[subprocess.Popen([python, script, "--c_id", str(cid)])] = command
            #procs.add(subprocess.Popen(["python3", script]))
        else:
            command = "{} {} --port {} --c_id {} --mix {} --debug {}".format(python, script, port, cid, mix, debug, mock_db)
            print(command)
            procs[(subprocess.Popen([python, script, "--port", str(port), "--c_id", str(cid), "--mix", str(mix), "--debug", str(debug), "--mock_db", str(mock_db)]))] = command #, stderr=subprocess.PIPE))

    try:
        # p.poll() only gets return code
        # p.communicate() can get stdout and stderr, but it blocks
        # i.e. a previous p in procs must finish before a later p's output can be accessed

        while procs:
            to_remove = set()
            for p in procs:
                if p.poll() is not None: # not None == finished
                    if p.returncode != 0: # abnoraml exit of a child process -> kill everything
                                        # default return code is 0
                        print("Abnormal exit:{}".format(procs[p]))
                        print(procs[p])
                        os.killpg(pid, signal.SIGTERM)
                    # else: correctly exited
                    to_remove.add(p)
            for p in to_remove:
                procs.pop(p) 
            time.sleep(2)
                    
    except:
        print("Run into exception, terminate parent process to terminate children")
        os.killpg(pid, signal.SIGTERM)

    print("All processes finished without errors.")
    

if __name__ == "__main__":
    # use ps aux | grep client.py to check how many clients are running
    parser = argparse.ArgumentParser()

    parser.add_argument("--mix", type=int, required=True)
    parser.add_argument("--range", nargs='+', required=True)
    parser.add_argument("--python", type=str, default="python3", help="Python alias to use")
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--mock_db", action='store_true')
    parser.add_argument("--ssh", action='store_true')
    args = parser.parse_args()

    mix = args.mix
    cid_range = args.range
    if (len(cid_range) != 2):
        print("This script takes exactly two argument! e.g. python launcher.py --range 0 10")
        sys.exit()

    cids = list(range(int(cid_range[0]), int(cid_range[1])))
    pid = os.getpid()
    # put launch_client into a separate process
    p = multiprocessing.Process(target=launch_client, args=(cids, mix, pid, args.python, int(args.debug), int(args.mock_db), args.ssh))
    p.start()

    start_time = time.time()
    while True:
        text = input()
        if text == "kill":
            os.killpg(pid, signal.SIGTERM)
        elif text == "status":
            if p.is_alive():
                print("Alive for {} seconds".format(int(time.time() - start_time)))
            else:
                print("All clients finished")
    


    # - add log so that we know what happened at abnormal exit

    
