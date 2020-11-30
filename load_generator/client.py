import socket
import datetime
import time
from random import seed
from random import randint
import argparse
import json

import web_to_sql
import con_data
import json_proc
import sql



HOST = "localhost"
TT = 3 # think time
MAX_TIME = 600
MAX_PROB = 9999
seed(1)
OK = "Ok"
NUM_ITEM = 1000
DEBUG = 1

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
        self.soc = None
        self.new_session = True

    def run(self):
        self.soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.soc.connect((HOST, self.port))
        print("Client {} connected at port {}".format(self.c_id, self.port))
        # 
        while datetime.datetime.now() < self.max_time:
            curr_url = con_data.urls[abbrs[self.curr]]
            print("Entering webpage {}".format(curr_url))
            # TODO: All comunication is plain text for now, will change to JSON
            # send BEGIN to start the transaction
            begin = web_to_sql.getBegin(curr_url)
            self.soc.sendall(begin)
            # TODO: check if we will get a response from backend (e.g. )
            data = self.soc.recv(2**24)
            if not json_proc.response_ok(data):
                print("Response contains error, terminating...")
                return 0
            # TODO: actually send the sql commands in order
            if curr_url == 'adminConf':
                ok = self.doAdminConf(s)
            elif curr_url == 'adminReq':
                ok = self.doAdminReq(s)
            elif curr_url == 'bestSell':
                ok = self.doBestSell(s)
            elif curr_url == 'buyConf':
                ok = self.doBuyConf(s)
            elif curr_url == 'buyReq':
                ok = self.doBuyReq(s)
            elif curr_url == 'custReg':
                ok = self.doCustReg(s)
            elif curr_url == 'home':
                ok = self.doHome(s)
            elif curr_url == 'newProd':
                ok = self.doNewProd(s)
            elif curr_url == 'orderDisp':
                ok = self.doOrderDisp(s)
            elif curr_url == 'orderInq':
                ok = self.doOrderInq(s)
            elif curr_url == 'prodDet':
                ok = self.doProdDet(s)
            elif curr_url == 'searchReq':
                ok = self.doSearchReq(s)
            elif curr_url == 'searchResult':
                ok = self.doSearchResult(s)
            elif curr_url == 'shopCart':
                ok = self.doShopCart(s)
            
            if not ok:
                print("Response contains error, terminating...")
                return 0
            
            # determine next state
            self.curr = determineNext(self.curr, self.mix)
            time.sleep(TT)


    # TODO: each response will be a JSON object through TCP
    # just use response["var_name"] to read back

    # Note: each function will take whatever retrieved by req.getParameter(varname) as argument 

    def doAdminConf(self):
        pass

    def doAdminReq(self):
        pass

    def doBestSell(self):
        pass

    def doBuyConf(self):
        pass

    def doBuyReq(self):
        pass

    def doCustReg(self):
        pass

    def doHome(self):
        # say hello - getName - c_id, shopping_id
        if self.new_session: # only getName when it is a new_session
            query = sql.replaceVars(sql.sqlNameToCommand["getName"], 1, self.c_id)
            response = self.send_query_and_receive_response(query)
            # DispOnly: TODO add c_fname and c_lname as class variables if needed else where
            if self.isErr(response):
                return False
            self.new_session = False
 
        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            return False

    def doNewProd(self):
        pass

    def doOrderDisp(self):
        pass

    def doOrderInq(self):
        pass

    def doProdDet(self):
        pass

    def doSearchReq(self):
        pass

    def doSearchResult(self):
        pass

    def doShopCart(self):
        # createEmptyCart (sequence)
        if not self.shopping_id: # only createEmptyCart (sequence) if no shopping_id yet
            # 1. createEmptyCart
            query = sql.sqlNameToCommand["createEmptyCart"]
            response = self.send_query_and_receive_response(query)
            # ReadResponse - read COUNT
            if self.isErr(response):
                return False
            self.shopping_id = int(response)

            # 2. createEmptyCartInsertV2
            query = sql.replaceVars(sql.sqlNameToCommand["createEmptyCartInsertV2"], 1, self.shopping_id)
            response = self.send_query_and_receive_response(query)
            # UpdateOnly:
            if self.isErr(response):
                return False

        # doCart (sequence)
        # 1. addItem (sequence) 
        #       - happens only when user set a flag, in which case only one i_id is given 
        #            -> use random number
        #       a. addItem
        #       b. addItemUpdate (if result not empty) or addItemPut (if result empty)
        flag = randint(0, 1)
        if flag:
            i_id = randint(0, NUM_ITEM-1)
            query = sql.replaceVars(sql.sqlNameToCommand["addItem"], 2, self.shopping_id, i_id)
            response = self.send_query_and_receive_response(query)
            # ReadResponse - read SELECT table
            if self.isErr(response):
                return False
            if self.isEmpty(response):
                # addItemPut
            else:
                # addItemUpdate
                response[0]["scl_qty"] = response[0]["scl_qty"] + 1

        # 2. refreshCart (sequnce)
        #       a. refreshCartRemove
        #       b. refreshCartUpdate


        # 3. addRandomItemToCartIfNecessary (sequence)
        #       a. addRandomItemToCartIfNecessary
        #       b. getRelated1

        # 4. resetCartTime

        # 5. getCart
        # TODO: function
        response = self.getCart()
        if self.isErr(response):
            return False

        # # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            return False

    def send_query_and_receive_response(self, query):
        # take raw query, return json result
        serialized = json_proc.construct_query(query)
        self.soc.sendall(serialized)
        response = self.soc.recv(2**24) # raw response
        if DEBUG:
            print(response)
        parsed = json.loads(response)
        if OK not in parsed:
            return "Err"
        result = response[OK]
        if not result:
            return "Empty"
        # only sql result, no rust layers
        return result
    
    def isErr(self, response):
        if response == "Err":
            return True
        else:
            return False
    
    def isEmpty(self, response):
        if response == "Empty":
            return True
        else:
            return False

    def getRelated(self):
        # getRelated - generate a random i_id (item id) as argument
        i_id = randint(0, NUM_ITEM-1)
        query = sql.replaceVars(sql.sqlNameToCommand["getRelated"], 1, i_id)
        response = self.send_query_and_receive_response(query)
        # DispOnly
        return response

    def getCart(self):
        pass


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
