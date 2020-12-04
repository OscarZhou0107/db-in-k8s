import sys
import subprocess
import os
import time
import argparse
import os
import signal

DEBUG = 1

'''
https://stackoverflow.com/questions/30200779/terminate-all-subprocesses-if-any-one-of-subprocess-has-error
use subprocess.Popen() to launch new scripts
add newly launched program into a list procs
then iterate through to find out if they are down

or set returncode. 

or use subprocess.check_output


for proc in procs:
    if proc.poll() is not None:  # it has terminated
        # check returncode and handle success / failure
'''


if __name__ == "__main__":
    # use ps aux | grep client.py to check how many clients are running
    parser = argparse.ArgumentParser()

    parser.add_argument("--range", nargs='+', required=True)
    args = parser.parse_args()
    cid_range = args.range
    if (len(cid_range) != 2):
        print("This script takes exactly two argument! e.g. python launcher.py --range 0 10")
        sys.exit()

    cids = list(range(int(cid_range[0]), int(cid_range[1])))
    
    script = "client.py"
    if DEBUG:
        script = "test.py"

    procs = {}
    port = 4000

    print("Popen child processes...")
    #try:
    for cid in cids:
        if DEBUG:
            command = "python3.8 {} --c_id {}".format(script, cid)
            procs[subprocess.Popen(["python3.8", script, "--c_id", str(cid)])] = command
            #procs.add(subprocess.Popen(["python3.8", script]))
        else:
            procs.add(subprocess.Popen("python3.8 {} --port {} --c_id {}".format(script, port, cid))) #, stderr=subprocess.PIPE))
            port = port + 1

    print(procs)
    # p.poll() only gets return code
    while procs:
        to_remove = set()
        for p in procs:
            if p.poll() is not None: # not None == finished
                if p.returncode != 0: # abnoraml exit of a child process -> kill everything
                                        # default return code is 0
                    print("abnormal exit")
                    print(procs[p])
                    os.killpg(os.getpid(), signal.SIGTERM)
                # else: correctly exited
                to_remove.add(p)
        for p in to_remove:
            procs.remove(p) 
                
    # p.communicate() can get stdout and stderr, but it blocks
    # i.e. a previous p in procs must finish before a later p's output can be accessed

    print("All processes finished without errors.")
    #finally: 
        # if ctrl-C, terminate all child processes
        #os.killpg(os.getpid(), signal.SIGTERM)



    
