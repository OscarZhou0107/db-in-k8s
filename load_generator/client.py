import socket
import datetime
import time
from random import seed
from random import randint
import argparse

import web_prob
import web_to_sql
import con_data


HOST = "localhost"
TT = 3 # think time
MAX_TIME = 600
MAX_PROB = 9999
seed(1)

def determineNext(curr, prob):
    row = prob[curr]
    value = randint(0, MAX_PROB)
    for i in range(len(row)):
        if value < row[i]:
            return i



class Client:
    def _init_(self, c_id, port, mix):
        self.c_id = c_id
        self.port = port
        self.shopping_id = None
        self.curr = "home"
        self.max_time = datetime.datetime.now() + datetime.timedelta(seconds=MAX_TIME)
        self.mix = mix
        else:
            print("client {}'s prob mix went wrong, terminating...".format(c_id)) 

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, self.port))
            print("Client {} connected at port {}".format(self.c_id, self.port))
            # 
            while datetime.datetime.now() < self.max_time:
                curr_url = con_data.urls[abbrs[self.curr]]
                print("Entering webpage {}".format(curr_url))
                # TODO: All comunication is plain text for now, will change to JSON
                # send BEGIN to start the transaction
                begin = web_to_sql.getBegin(curr_url)
                s.sendall(begin)
                data = s.recv(2**24)
                # TODO: actually execute the sql commands in order
                # determine next state
                self.curr = determineNext(self.curr, self.mix)
                time.sleep(TT)



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int)
    parser.add_argument("--c_id", type=int)
    parser.add_argument("--mix", type=int, default=0)
    args = parser.parse_args()
    if args.mix == 0:
        mix = con_data.fake
    elif args.mix == 1:
        mix = con_data.mix1
    elif args.mix == 2:
        mix = con_data.mix2
    elif args.mix == 3:
        mix = con_data.mix3
    else:
        print("Wrong mix number! Teminating...")
        return 0
    # Check mix dimension
    if len(mix) != len(mix[0]):
        print("Probability table is not square! Terminating...")
        return 0

    newClient = Client(args.c_id, args.port, mix)
    newClient.run()
