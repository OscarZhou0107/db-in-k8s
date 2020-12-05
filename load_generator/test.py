import argparse
import time 
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--c_id", type=int, default=0)
    args = parser.parse_args()
    
    c_id = args.c_id
    print("{} started".format(c_id))
    if c_id == 0:
        time.sleep(3)
        print("{} finished".format(c_id))
        #sys.exit(1)
    else:
        time.sleep(3)
        print("{} finished".format(c_id))
