import logging
import argparse

def wait_for_input(cid_range):
    while True:
        text = input("input: \n")
        logging.info("logging received {}".format(text))
        if "kill" in text:
            print("{} received kill".format(cid_range))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--range", nargs='+', required=True)
    args = parser.parse_args()
    cid_range = args.range

    logname = "".join(cid_range)+"test.log"
    logging.basicConfig(filename=logname,
                        filemode='w',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.DEBUG)

    logger = logging.getLogger('urbanGUI')
    
    wait_for_input(cid_range)