import socket
import datetime
import time
from random import seed
from random import randint
from random import uniform
import argparse
import json
import csv
import sys
import logging
import os

import web_to_sql
import con_data
import sql

TT = 0
#TT = 0.5 # think time
MAX_TIME = 6000
MAX_PROB = 9999
OK = "Ok"
NUM_ITEM = 1000
NUM_QTY = 10
NUM_PAIR = 10
WRONG_PASSWD_FRENQUENCY = 1 # out of 10
MAX_STRING_LEN = 10
MAX_NUM_LEN = 12
UPDATE_NO_ERR_CHECK = 1
ALLOW_ABORT = 1

def determineNext(curr_index, prob):
    row = prob[curr_index]
    value = randint(0, MAX_PROB)
    for i in range(len(row)):
        if value <= row[i]:
            return con_data.states[i]

def generateRandomString():
    res = []
    allChar = [ 'a','b','c','d','e','f','g','h','i','j','k',
                'l','m','n','o','p','q','r','s','t','u','v',
                'w','x','y','z','A','B','C','D','E','F','G',
                'H','I','J','K','L','M','N','O','P','Q','R',
                'S','T','U','V','W','X','Y','Z','!','@','#',
                '$','%','^','&','*','(',')','_','-','=','+',
                '{','}','[',']','|',':',';',',','.','?','/',
                '~',' ' ]
    for i in range(randint(1, MAX_STRING_LEN)):
        index = randint(0,len(allChar)-1)
        res.append(allChar[index])
    return "'" + "".join(res) + "'"

def generateRandomNum():
    res = []
    allChar = "1234567890"
    for i in range(randint(1, MAX_NUM_LEN)):
        index = randint(0,len(allChar)-1)
        res.append(allChar[index])
    return int("".join(res))

def generateRandomSubject():
    subjects = ["""'ARTS'""", """'BIOGRAPHIES'""", """'BUSINESS'""", """'CHILDREN'""", """'COMPUTERS'""", """'COOKING'""", 
                """'HEALTH'""", """'HISTORY'""", """'HOME'""", """'HUMOR'""", """'LITERATURE'""", """'MYSTERY'""", """'NON-FICTION'""", 
                """'PARENTING'""", """'POLITICS'""", """'REFERENCE'""", """'RELIGION'""", """'ROMANCE'""", """'SCIENCE-FICTION'""", 
                """'SCIENCE-NATURE'""", """'SELF-HELP'""", """'SPORTS'""", """'TRAVEL'""", """'YOUTH'"""]
    index = randint(0, len(subjects)-1)
    return subjects[index]

def generateRandomCountry():
    countries = [
        """'Algeria'""",
        """'Argentina'""",
        """'Armenia'""",
        """'Australia'""",
        """'Austria'""",
        """'Azerbaijan'""",
        """'Bahamas'""",
        """'Bahrain'""",
        """'Bangla Desh'""",
        """'Barbados'""",
        """'Belarus'""",
        """'Belgium'""",
        """'Bermuda'""",
        """'Bolivia'""",
        """'Botswana'""",
        """'Brazil'""",
        """'Bulgaria'""",
        """'Canada'""",
        """'Cayman Islands'""",
        """'Chad'""",
        """'Chile'""",
        """'China'""",
        """'Christmas Island'""",
        """'Colombia'""",
        """'Croatia'""",
        """'Cuba'""",
        """'Cyprus'""",
        """'Czech Republic'""",
        """'Denmark'""",
        """'Dominican Republic'""",
        """'Eastern Caribbean'""",
        """'Ecuador'""",
        """'Egypt'""",
        """'El Salvador'""",
        """'Estonia'""",
        """'Ethiopia'""",
        """'Falkland Island'""",
        """'Faroe Island'""",
        """'Fiji'""",
        """'Finland'""",
        """'France'""",
        """'Gabon'""",
        """'Germany'""",
        """'Gibraltar'""",
        """'Greece'""",
        """'Guam'""",
        """'Hong Kong'""",
        """'Hungary'""",
        """'Iceland'""",
        """'India'""",
        """'Indonesia'""",
        """'Iran'""",
        """'Iraq'""",
        """'Ireland'""",
        """'Israel'""",
        """'Italy'""",
        """'Jamaica'""",
        """'Japan'""",
        """'Jordan'""",
        """'Kazakhstan'""",
        """'Kuwait'""",
        """'Lebanon'""",
        """'Luxembourg'""",
        """'Malaysia'""",
        """'Mauritius'""",
        """'Mexico'""",
        """'Netherlands'""",
        """'New Zealand'""",
        """'Norway'""",
        """'Pakistan'""",
        """'Philippines'""",
        """'Poland'""",
        """'Portugal'""",
        """'Romania'""",
        """'Russia'""",
        """'Saudi Arabia'""",
        """'Singapore'""",
        """'Slovakia'""",
        """'South Africa'""",
        """'South Korea'""",
        """'Spain'""",
        """'Sudan'""",
        """'Sweden'""",
        """'Switzerland'""",
        """'Taiwan'""",
        """'Thailand'""",
        """'Trinidad'""",
        """'Turkey'""",
        """'United Kingdom'""",
        """'United States'""",
        """'Venezuela'""",
        """'Zambia'"""
    ]
    index = randint(0, len(countries)-1)
    return countries[index]

def jsonToByte(serialized):
    # input is a dictionary converted to string by json.dumps()
    # first 4 bytes (big endian) need to be the length of the remaining message
    msgLen = len(serialized).to_bytes(4, byteorder='big')
    encoded = msgLen + serialized.encode('utf-8')
    return encoded

def byteToJson(raw):
    return json.loads((raw[4:]).decode('utf-8'))


class Client:
    def __init__(self, c_id, port, mix):
        self.c_id = c_id
        self.port = port
        self.curr = "home"
        self.max_time = datetime.datetime.now() + datetime.timedelta(seconds=MAX_TIME)
        self.mix = mix
        self.load = False # will be updated in getName in doHome -> if True, existing customer with info loaded
        self.soc = None # will be updated once run() is called on a client
        self.shopping_id = None # will be updated in createEmptyCart in doShopCart
        self.c_uname = None # will be updated by getUserName in doCustReg, or by createNewCustomer in doBuyReq
        
        # set up logging
        # check if the log folder exist
        if not os.path.isdir("./logs"):
            os.mkdir("./logs")
        logname = "./logs/client_" + str(c_id) + "_process_" + str(os.getpid()) + ".log"
        handlers = [logging.FileHandler(logname, mode="w")]
        if DEBUG:
            handlers.append(logging.StreamHandler(sys.stdout))
        logging.basicConfig(
            level=logging.DEBUG,
            #level=logging.WARNING,
            format='%(asctime)s,%(msecs)d %(name)s [%(levelname)s] %(message)s',
            # write to both stdout and log file
            handlers=handlers
        )
        # set name for this logger
        self.logger = logging.getLogger("client_" + str(c_id) + "_process_" + str(os.getpid()))
        
        # set up random seed
        seed(c_id)

    def run(self):
        self.soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("### connect to {} at port {}".format(HOST, self.port))
        self.soc.connect((HOST, self.port))
        self.logger.info("Client {} in process {} connected at port {}".format(self.c_id, os.getpid(), self.port))

        while datetime.datetime.now() < self.max_time:
            curr_index = con_data.states.index(self.curr)
            self.logger.debug("=======================================")
            self.logger.critical("Entering webpage {}".format(self.curr))

            single_read = ["searchReq", "prodDet", "custReg", "adminReq"]
            # skip sending BEGIN if single read
            if not self.curr in single_read:
                # send BEGIN to start the transaction
                begin = web_to_sql.getBegin(self.curr)
                self.logger.info("### Sending data: BEGIN")
                self.logger.debug(begin)
                self.soc.sendall(jsonToByte(begin))

                # receive response to BEGIN
                try:
                    data = byteToJson(self.soc.recv(2**24))
                    self.logger.info("### Receiving data: BEGIN")
                    self.logger.debug(data)

                    if OK not in data["reply"]["BeginTx"]:
                        self.logger.error("Begin response contains error, terminating...")
                        return 0
                except:
                    print("begin tcp receiving failed")

            
            if self.curr == 'adminConf':
                okay = self.doAdminConf()
            elif self.curr == 'adminReq':
                okay = self.doAdminReq()
            elif self.curr == 'bestSell':
                okay = self.doBestSell()
            elif self.curr == 'buyConf':
                okay = self.doBuyConf()
            elif self.curr == 'buyReq':
                okay = self.doBuyReq()
            elif self.curr == 'custReg':
                okay = self.doCustReg()
            elif self.curr == 'home':
                okay = self.doHome()
            elif self.curr == 'newProd':
                okay = self.doNewProd()
            elif self.curr == 'orderDisp':
                okay = self.doOrderDisp()
            elif self.curr == 'orderInq':
                okay = self.doOrderInq()
            elif self.curr == 'prodDet':
                okay = self.doProdDet()
            elif self.curr == 'searchReq':
                okay = self.doSearchReq()
            elif self.curr == 'searchResult':
                okay = self.doSearchResult()
            elif self.curr == 'shopCart':
                okay = self.doShopCart()
            
            if not okay:
                self.logger.error("Response during {} contains error, crashing server...".format(self.curr))
                crash = web_to_sql.getCrash(self.curr)
                self.soc.sendall(jsonToByte(crash))
                return 0

            # skip sending COMMIT if single read
            if not self.curr in single_read:
                # send commit to end current transaction
                commit = web_to_sql.getCommit()
                self.logger.info("### Sending data: COMMIT")
                self.logger.debug(commit)
                self.soc.sendall(jsonToByte(commit))

                # receive reponse to commit
                try:
                    data = byteToJson(self.soc.recv(2**24))
                    self.logger.info("### Receiving data: COMMIT")
                    self.logger.debug(data)

                    if OK not in data["reply"]["EndTx"]:
                        self.logger.error("End response contains error, terminating...")
                        return 0
                except:
                    print("commit receiving failed")
            
            # determine next state
            self.curr = determineNext(curr_index, self.mix)
            time.sleep(TT)
        
        return 1

    '''
    ==================================================================================================================
    ==================================================================================================================
    ==================================================================================================================
    '''

    # Each response will be a JSON object through TCP
    # actual result from sql will be in csv format

    # Note: each function will take whatever retrieved by req.getParameter(varname) as argument 
    # sql response types:
    #   - DispOnly - SELECT, but no need to read data from response
    #   - UpdateOnly -> INSERT, DELETE, UPDATE...
    #   - ReadResponse -> need to read data from response
    

    def doAdminConf(self): # state 0
        # getBook
        i_id = randint(1, NUM_ITEM)
        response = self.getBook(i_id)
        # DispOnly
        if self.isErr(response):
            self.logger.error("Response to getBook has error")
            return False
        
        # adminUpdate (sequence)
        #   a. adminUpdate
        image = generateRandomString()
        thumbnail = generateRandomString()
        cost = round(uniform(1, 1500), 2)
        item_info = [cost, image, thumbnail, i_id]
        query = sql.replaceVars(sql.sqlNameToCommand["adminUpdate"], 4, item_info)
        response = self.send_query_and_receive_response(query, "adminUpdate")
        # UpdateOnly
        if not UPDATE_NO_ERR_CHECK and self.isErr(response):
            self.logger.error("Response to adminUpdate has error")
            return False

        #   b. adminUpdateRelated
        query = sql.replaceVars(sql.sqlNameToCommand["adminUpdateRelated"], 2, [i_id, i_id])
        # EarlyRelease - orders, order_line
        ertables = ["orders", "order_line"]
        response = self.send_query_and_receive_response(query, "adminUpdateRelated", ertables)
        # ReadResponse - SELECT ol_i_id FROM orders, order_line
        if self.isAbort(response):
            self.logger.warning("Response to adminUpdateRelated is abort")
            return True
        if self.isErr(response):
            self.logger.error("Response to adminUpdateRelated has error")
            return False
        if self.isEmpty(response):
            self.logger.warning("Response to adminUpdateRelated is empty")
            return True # bypass the rest
        
        # add exactly 5 items into related
        num = len(response)
        related = []
        if num >= 5:
            num = 5
        else: # if there are under 5 items in response, add the first item multiple times
            for i in range(5-num):
                related.append(response[0][0])
        for i in range(num):
            related.append(response[i][0])
        # then add i_id
        related.append(i_id)

        #   c. adminUpdateRelated1
        query = sql.replaceVars(sql.sqlNameToCommand["adminUpdateRelated1"], 6, related)
        response = self.send_query_and_receive_response(query, "adminUpdateRelated1")
        # UpdateOnly
        if not UPDATE_NO_ERR_CHECK and self.isErr(response):
            self.logger.error("Response to adminUpdateRelated1 has error")
            return False

        return True

    def doAdminReq(self): # state 1
        # getBook
        response = self.getBook(-1)
        # DispOnly
        if self.isErr(response):
            self.logger.error("Response to getBook has error")
            return False
        return True

    def doBestSell(self): # state 2
        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            self.logger.error("Response to getRelated has error")
            return False
        
        # getBestSellers
        subject = generateRandomSubject()
        query = sql.replaceVars(sql.sqlNameToCommand["getBestSellers"], 1, [subject])
        response = self.send_query_and_receive_response(query, "getBestSellers")
        # DispOnly
        if self.isErr(response):
            self.logger.error("Response to getBestSellers has error")
            return False
        
        return True

    def doBuyConf(self): # state 3
        # getCDiscount
        query = sql.replaceVars(sql.sqlNameToCommand["getCDiscount"], 1, [self.c_id])
        response = self.send_query_and_receive_response(query, "getCDiscount")
        # ReadResponse - SELECT c_discount
        if self.isAbort(response):
            self.logger.warning("Response to getCDiscount is abort")
            return True
        if self.isErr(response):
            self.logger.error("Response to getCDiscount has error")
            return False
        if self.isEmpty(response):
            self.logger.warning("Response to getCDiscount is empty")
            return True # bypass the rest

        discount = float(response[0][0])

        # getCart
        if self.shopping_id:
            response = self.getCart()
            # ReadResponse - cart
            if self.isAbort(response):
                self.logger.warning("Response to getCart is abort")
                return True
            if self.isErr(response):
                self.logger.error("Response to getCart has error")
                return False
            if self.isEmpty(response):
                return True
            processed = self.processCart(response, discount) # handle all index and return a dictionary

            ship_addr_id = 0
            # choose between 2 options by random
            if randint(0, 1):
                # enterAddress
                street1 = generateRandomString()
                street2 = generateRandomString()
                city = generateRandomString()
                state = generateRandomString()
                zzip = generateRandomString()
                country = generateRandomCountry()
                response = self.enterAddress(street1, street2, city, state, zzip, country)
                # ReadResponse - addr_id (self constructed in EnterAddress)
                if self.isAbort(response):
                    self.logger.warning("Response to enterAddress is abort")
                    return True
                if self.isErr(response):
                    self.logger.error("Response to enterAddress has error")
                    return False
                if self.isEmpty(response):
                    self.logger.warning("Response to enterAddress is empty")
                    return False
                ship_addr_id = response[0][0]
            else:
                # getCAddr
                query = sql.replaceVars(sql.sqlNameToCommand["getCAddr"], 1, [self.c_id])
                response = self.send_query_and_receive_response(query, "getCAddr")
                # ReadResponse - SELECT c_addr_id
                if self.isAbort(response):
                    self.logger.warning("Response to getCAddr is abort")
                    return True
                if self.isErr(response):
                    self.logger.error("Response to getCAddr has error")
                    return False
                if self.isEmpty(response):
                    self.logger.warning("Response to getCAddr is empty")
                    return True # bypass the rest

                ship_addr_id = response[0][0]

            # enterOrder (sequence)
            #   a. getCAddrId - same as getCAddr
            query = sql.replaceVars(sql.sqlNameToCommand["getCAddrId"], 1, [self.c_id])
            # EarlyRelease - customer
            ertables = ["customer"]
            response = self.send_query_and_receive_response(query, "getCAddrId", ertables)
            # ReadResponse - SELECT c_addr_id
            if self.isAbort(response):
                self.logger.warning("Response to getCAddrId is abort")
                return True
            if self.isErr(response):
                self.logger.error("Response to getAddrId has error")
                return False
            if self.isEmpty(response):
                self.logger.warning("Response to getAddrId is empty")
                return True # bypass the rest

            c_addr_id = response[0][0]

            #   b. enterOrderMaxId
            query = sql.sqlNameToCommand["enterOrderMaxId"]
            response = self.send_query_and_receive_response(query, "enterOrderMaxId")
            # ReadResponse - SELECT count(o_id)
            if self.isAbort(response):
                self.logger.warning("Response to enterOrderMaxId is abort")
                return True
            if self.isErr(response):
                self.logger.error("Response to enterOrderMaxId has error")
                return False
            if self.isEmpty(response):
                self.logger.warning("Response to enterOrderMaxId is empty")
                return False # count has to have a number
            
            o_id = int(response[0][0]) + 1

            #   c. enterOrderInsert
            o_sub_total = processed["sc_sub_total"]
            o_total = processed["sc_total"]
            ship_type = ["""'FEDEX'""", """'SHIP'""", """'AIR'""", """'COURIER'""", """'UPS'""", """'MAIL'"""]
            o_ship_type = randint(0, len(ship_type)-1)
            interval = randint(1, 7)
            order_info = [o_id, self.c_id, o_sub_total, o_total, o_ship_type, interval, c_addr_id, ship_addr_id]
            query = sql.replaceVars(sql.sqlNameToCommand["enterOrderInsert"], 8, order_info)
            # EarlyRelease - orders
            ertables = ["orders"]
            response = self.send_query_and_receive_response(query, "enterOrderInsert", ertables)
            # UpdateOnly
            if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                self.logger.error("Response to enterOrderInsert has error")
                return False

            # loop
            for i in range(len(processed["lines"])):
                # addOrderLine
                orderline_info = [i, o_id, processed["lines"][i]["scl_i_id"], 
                                  processed["lines"][i]["scl_qty"], discount, 
                                  generateRandomString()]
                query = sql.replaceVars(sql.sqlNameToCommand["addOrderLine"], 6, orderline_info)
                # EarlyRelease - order_line
                ertables = []
                if i == len(processed["lines"]) - 1:
                    ertables = ["order_line"]
                response = self.send_query_and_receive_response(query, "addOrderLine", ertables)
                # UpdateOnly
                if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                    self.logger.error("Response to addOrderLine has error")
                    return False

                # getStock
                query = sql.replaceVars(sql.sqlNameToCommand["getStock"], 1, [processed["lines"][i]["scl_i_id"]])
                response = self.send_query_and_receive_response(query, "getStock")
                # ReadResponse - SELECT i_stock
                if self.isAbort(response):
                    self.logger.warning("Response to getStock is abort")
                    return True
                if self.isErr(response):
                    self.logger.error("Response to getStock has error")
                    return False
                if self.isEmpty(response):
                    self.logger.warning("Response to getStock is empty")
                    return True # bypass the rest

                stock = int(response[0][0])

                # setStock
                if stock - processed["lines"][i]["scl_qty"] < 10:
                    stock = stock - processed["lines"][i]["scl_qty"] + 21
                else:
                    stock = stock - processed["lines"][i]["scl_qty"]
                query = sql.replaceVars(sql.sqlNameToCommand["setStock"], 2, [stock, processed["lines"][i]["scl_i_id"]])
                # EarlyRelease - item
                ertables = []
                if i == len(processed["lines"]) - 1:
                    ertables = ["item"]
                response = self.send_query_and_receive_response(query, "setStock", ertables)
                # UpdateOnly
                if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                    self.logger.error("Response to setStock has error")
                    return False

            # enterCCXact
            allType = ["""'DISCOVER'""", """'DINERS'""", """'VISA'""", """'AMEX'""", """'MASTERCARD'"""]
            cc_type = allType[randint(0, len(allType)-1)]
            cc_num = generateRandomNum()
            cc_name = generateRandomString()
            cc_expiry = "'" + str((datetime.datetime.now() + datetime.timedelta(days=365)).strftime('%Y-%m-%d %H:%M:%S')) + "'"
            cc_info = [o_id, cc_type, cc_num, cc_name, cc_expiry, o_total, ship_addr_id]
            query = sql.replaceVars(sql.sqlNameToCommand["enterCCXact"], 7, cc_info)
            # EarlyRelease - country, cc_xacts, address
            ertables = ["country", "cc_xacts", "address"]
            response = self.send_query_and_receive_response(query, "enterCCXact", ertables)
            # UpdateOnly
            if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                self.logger.error("Response to enterCCXact has error")
                return False

            # clearCart
            query = sql.replaceVars(sql.sqlNameToCommand["clearCart"], 1, [self.shopping_id])
            response = self.send_query_and_receive_response(query, "clearCart")
            # UpdateOnly
            if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                self.logger.error("Response to clearCart has error")
                return False

        return True

    def doBuyReq(self): # state 4
        flag = randint(0, 1)
        if (flag == 1): # only if flag is Y == 1
            if self.load and self.c_uname: # only if both c_uname and c_passwd are given (implied by load)
                # getCustomer
                query = sql.replaceVars(sql.sqlNameToCommand["getCustomer"], 1, ["'" + self.c_uname.strip("'") + "'"])
                response = self.send_query_and_receive_response(query, "getCustomer")
                # DispOnly
                if self.isErr(response):
                    self.logger.error("Response to getCustomer has error")
                    return False

                # refreshSession 
                query = sql.replaceVars(sql.sqlNameToCommand["refreshSession"], 1, [self.c_id])
                response = self.send_query_and_receive_response(query, "refreshSession")
                # UpdateOnly
                if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                    self.logger.error("Response to refreshSession has error")
                    return False

                # use random number to simulate if input password is incorrect
                if randint(1, WRONG_PASSWD_FRENQUENCY) == 1:
                    return True
        # only if flag is N == 0
        else:
            # createNewCustomer (sequence)
            #   1. enterAddress
            street1 = generateRandomString()
            street2 = generateRandomString()
            city = generateRandomString()
            state = generateRandomString()
            zzip = generateRandomString()
            country = generateRandomCountry()
            response = self.enterAddress(street1, street2, city, state, zzip, country)
            # ReadResponse - addr_id (self constructed in EnterAddress)
            if self.isAbort(response):
                self.logger.warning("Response to enterAddress is abort")
                return True
            if self.isErr(response):
                self.logger.error("Response to enterAddress has error")
                return False
            if self.isEmpty(response):
                self.logger.warning("Response to enterAddress is empty")
                return False
            addr_id = response[0][0]
           
            #   2. createNewCustomerMaxId
            query = sql.sqlNameToCommand["createNewCustomerMaxId"]
            response = self.send_query_and_receive_response(query, "createNewCustomerMaxId")
            # ReadResponse - SELECT max(c_id)
            if self.isAbort(response):
                self.logger.warning("Response to createNewCustomerMaxId is abort")
                return True
            if self.isErr(response):
                self.logger.error("Response to createNewCustomerMaxId has error")
                return False
            if self.isEmpty(response):
                self.logger.warning("Response to createNewCustomerMaxId is empty")
                return False # max has to have a number
            max_id = int(response[0][0]) + 1

            #   3. createNewCustomer
            #       - fields:
            #              id, c_uname, c_passwd, c_fname, c_lname, 
            #              c_addr_id, c_phone, c_email, c_since, c_last_login, 
            #              c_login, c_expiration, c_discount, c_balance, c_ytd_pmt, 
            #              c_birthdate, c_data
            c_uname = generateRandomString()
            self.c_uname = c_uname
            c_passwd = c_uname
            c_fname = generateRandomString()
            c_lname = generateRandomString()
            c_phone = generateRandomNum()
            c_email = generateRandomString()
            c_last_login = "'" + str(datetime.datetime.now().strftime('%Y-%m-%d')) + "'" 
            c_since = "'" + str(datetime.datetime.now().strftime('%Y-%m-%d')) + "'"
            c_login = "'" + str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')) + "'"
            c_expiration = "'" + str((datetime.datetime.now() + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')) + "'"
            c_discount = randint(0, 50) * 1.0
            c_birthdate = "'" + str(datetime.datetime.now().strftime('%Y-%m-%d')) + "'"
            c_data = generateRandomString()
            cust_info = [   max_id, c_uname, c_passwd, c_fname, c_lname, 
                            addr_id, c_phone, c_email, c_since, c_last_login,
                            c_login, c_expiration, c_discount, 0.0, 0.0, 
                            c_birthdate, c_data
                        ]
            query = sql.replaceVars(sql.sqlNameToCommand["createNewCustomer"], 17, cust_info)
            # EarlyRelease - customer
            ertables = ["customer"]
            response = self.send_query_and_receive_response(query, "createNewCustomer", ertables)
            # UpdateOnly
            if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                self.logger.error("Response to createNewCustomer has error")
                return False

        # getCart
        if self.shopping_id:
            response = self.getCart()
            if self.isErr(response):
                self.logger.error("Response to getCart has error")
                return False

        return True

    def doCustReg(self): # state 5
        # getUserName
        query = sql.replaceVars(sql.sqlNameToCommand["getUserName"], 1, [self.c_id])
        response = self.send_query_and_receive_response(query, "getUserName")
        # ReadResponse - SELECT c_uname
        if self.isAbort(response):
            self.logger.warning("Response to getUserName is abort")
            return True
        if self.isErr(response):
            self.logger.error("Response to getUserName has error")
            return False
        if self.isEmpty(response):
            self.logger.warning("Response to getUserName is empty")
            return True # bypass the rest
        self.c_uname = response[0][0]

        return True

    def doHome(self): # state 6
        # say hello - getName - c_id, shopping_id
        if not self.load: # only getName if it is a new connection or if the customer is not in the db
            query = sql.replaceVars(sql.sqlNameToCommand["getName"], 1, [self.c_id])
            # EarlyRelease - customer
            ertables = ["customer"]
            response = self.send_query_and_receive_response(query, "getName", ertables)
            # ReadResponse - if not empty, existing customer, load data
            if self.isAbort(response):
                self.logger.warning("Response to getName is abort")
                return True
            if self.isErr(response):
                self.logger.error("Response to getName has error")
                return False
            if not self.isEmpty(response):
                self.load = True
            else:
                self.logger.warning("Response to getName is empty")

        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            self.logger.error("Response to getRelated has error")
            return False
        
        return True

    def doNewProd(self): # state 7
        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            self.logger.error("Response to getRelated has error")
            return False

        # getNewProducts
        subject = generateRandomSubject()
        query = sql.replaceVars(sql.sqlNameToCommand["getNewProducts"], 1, [subject])
        response = self.send_query_and_receive_response(query, "getNewProducts")
        # DispOnly
        if self.isErr(response):
            self.logger.error("Response to getNewProducts has error")
            return False

        return True

    def doOrderDisp(self): # state 8
        # 1. getPassword 
        if self.load and self.c_uname: # only if both c_uname and c_passwd are given (implied by load)
            query = sql.replaceVars(sql.sqlNameToCommand["getPassword"], 1, ["'" + self.c_uname.strip("'") + "'"])
            response = self.send_query_and_receive_response(query, "getPassword")
            # DispOnly
            if self.isErr(response):
                self.logger.error("Response to getPassword has error")
                return False
            
            # use random number to simulate if input password is correct
            if randint(1, WRONG_PASSWD_FRENQUENCY)> 1: 
                # 2. getMostRecentOrder (sequence) - only execute if input password is correct
                #       a. getMostRecentOrderId
                #       b. getMostRecentOrderOrder - only execute if a. is not empty; need o_id from a.
                #       c. getMostRecentOrderLines - only execyte if b. is not empty; need o_id from a.
                    # getMostRecentOrderId
                query = sql.replaceVars(sql.sqlNameToCommand["getMostRecentOrderId"], 1, ["'" + self.c_uname.strip("'") + "'"])
                response = self.send_query_and_receive_response(query, "getMostRecentOrderId")
                # ReadResponse - SELECT o_id
                if self.isAbort(response):
                    self.logger.warning("Response to getMostRecentOrderId is abort")
                    return True
                if self.isErr(response):
                    self.logger.error("Response to getMostRecentOrderId has error")
                    return False
                if not self.isEmpty(response):
                    o_id = response[0][0]
                    # getMostRecentOrderOrder
                    query = sql.replaceVars(sql.sqlNameToCommand["getMostRecentOrderOrder"], 1, [o_id])
                    # EarlyRelease - customer, country, cc_xacts, orders, address
                    ertables = ["customer", "country", "cc_xacts", "orders", "address"]
                    response = self.send_query_and_receive_response(query, "getMostRecentOrderOrder", ertables)
                    # ReadResponse - SELECT orders.*, customer.*; but only care if it is empty
                    if self.isAbort(response):
                        self.logger.warning("Response to getMostRecentOrderOrder is abort")
                        return True
                    if self.isErr(response):
                        self.logger.error("Response to getMostRecentOrderOrder has error")
                        return False
                    if not self.isEmpty(response):
                        query = sql.replaceVars(sql.sqlNameToCommand["getMostRecentOrderLines"], 1, [o_id])
                        response = self.send_query_and_receive_response(query, "getMostRecentOrderLines")
                        # DispOnly
                        if self.isErr(response):
                            self.logger.error("Response to getMostRecentOrderLine has error")
                            return False
                    else:
                        self.logger.warning("Response to getMostRecentOrderOrder is empty")
                else:
                    self.logger.warning("Response to getMostRecentOrderId is empty")

        return True

    def doOrderInq(self): # state 9
        return True

    def doProdDet(self): # state 10
        response = self.getBook(-1)
        # DispOnly
        if self.isErr(response):
            self.logger.error("Response to getBook has error")
            return False
        return True

    def doSearchReq(self): # state 11
        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            self.logger.error("Response to getRelated has error")
            return False
        return True

    def doSearchResult(self): # state 12
        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            self.logger.error("Response to getRelated has error")
            return False
        
        # choose one of three searches to do -> use a random number to decide
        # doAuthorSearch
        # doTitleSearch
        # doSubjectSearch
        #searchType = randint(1, 3)
        searchType = 3 # hack: since we cannot get soundex to work, skip queries using it
        if searchType == 1:
            # author
            searchKey = generateRandomString()
            query = sql.replaceVars(sql.sqlNameToCommand["doAuthorSearch"], 1, [searchKey])
            response = self.send_query_and_receive_response(query, "doAuthorSearch")
            # DispOnly
            if self.isErr(response):
                self.logger.error("Response to doAuthorSearch has error")
                return False
        elif searchType == 2:
            # title
            searchKey = generateRandomString()
            query = sql.replaceVars(sql.sqlNameToCommand["doTitleSearch"], 1, [searchKey])
            response = self.send_query_and_receive_response(query, "doTitleSearch")
            # DispOnly
            if self.isErr(response):
                self.logger.error("Response to doTitleSearch has error")
                return False
        elif searchType == 3: # searchType == 3
            # subject
            searchKey = generateRandomSubject()
            query = sql.replaceVars(sql.sqlNameToCommand["doSubjectSearch"], 1, [searchKey])
            response = self.send_query_and_receive_response(query, "doSubjectSearch")
            # DispOnly
            if self.isErr(response):
                self.logger.error("Response to doSubjectSearch has error")
                return False

        return True

    def doShopCart(self): # state 13
        # createEmptyCart (sequence)
        if not self.shopping_id: # only createEmptyCart (sequence) if no shopping_id yet
            # 1. createEmptyCart
            query = sql.sqlNameToCommand["createEmptyCart"]
            response = self.send_query_and_receive_response(query, "createEmptyCart")
            # ReadResponse - read COUNT
            if self.isAbort(response):
                self.logger.warning("Response to createEmptyCart is abort")
                return True
            if self.isErr(response):
                self.logger.error("Response to createEmptyCart has error")
                return False
            if self.isEmpty(response):
                self.logger.warning("Response to createEmptyCart is empty")
                return False # count has to have a number
            self.shopping_id = int(response[0][0])

            # 2. createEmptyCartInsertV2
            query = sql.replaceVars(sql.sqlNameToCommand["createEmptyCartInsertV2"], 1, [self.shopping_id])
            response = self.send_query_and_receive_response(query, "createEmptyCartInsertV2")
            # UpdateOnly:
            if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                self.logger.error("Response to createEmptyCartInsertV2 has error")
                return False

        # doCart (sequence)
        # 1. addItem (sequence) 
        #       - happens only when user set a flag, in which case only one i_id is given -> use random number
        flag = randint(0, 1)
        if flag:
            response = self.addItem(-1)
            if self.isErr(response):
                self.logger.error("Response to addItem has error")
                return False

        # 2. refreshCart (sequnce)
        #       - happens only when no user flag is set, and number of (Qty, i_id) > 0
        #       a. refreshCartRemove - if Qty is 0
        #       b. refreshCartUpdate - if Qty  > 0
        else:
            # generate a random number of pairs (Qty, i_id), each element a random number 
            numPair = randint(0, NUM_PAIR)
            for i in range(numPair):
                qty = randint(0, NUM_QTY)
                iid = randint(1, NUM_ITEM)
                if qty == 0:
                    query = sql.replaceVars(sql.sqlNameToCommand["refreshCartRemove"], 2, [self.shopping_id, iid])
                    response = self.send_query_and_receive_response(query, "refreshCartRemove")
                    # UpdateOnly
                    if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                        self.logger.error("Response to refreshCartRemove has error")
                        return False
                else:
                    query = sql.replaceVars(sql.sqlNameToCommand["refreshCartUpdate"], 3, [qty, self.shopping_id, iid])
                    response = self.send_query_and_receive_response(query, "refreshCartUpdate")
                    # UpdateOnly
                    if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                        self.logger.error("Response to refreshCartUpdate has error")
                        return False

        # 3. addRandomItemToCartIfNecessary (sequence)
        #       a. addRandomItemToCartIfNecessary
        #       b. getRelated1 - b.c. only if a. returned 0
        #       c. addItem
        query = sql.replaceVars(sql.sqlNameToCommand["addRandomItemToCartIfNecessary"], 1, [self.shopping_id])
        response = self.send_query_and_receive_response(query, "addRandomItemToCartIfNecessary")
        # ReadResponse - read COUNT
        if self.isAbort(response):
            self.logger.warning("Response to addRandomItemToCartIfNecessary is abort")
            return True
        if self.isErr(response):
            self.logger.error("Response to addRandomItemToCartIfNecessary has error")
            return False
        if self.isEmpty(response):
            self.logger.warning("Response to addRandomItemToCartIfNecessary is empty")
            return False # count has to have a number
        count = int(response[0][0])

        if count == 0:
            i_id = randint(1, NUM_ITEM)
            query = sql.replaceVars(sql.sqlNameToCommand["getRelated1"], 1, [i_id])
            response = self.send_query_and_receive_response(query, "getRelated1")
            # ReadResponse - read SELECT i_related1
            if self.isAbort(response):
                self.logger.warning("Response to getRelated1 is abort")
                return True
            if self.isErr(response):
                self.logger.error("Response to getRelated1 has error")
                return False
            if not self.isEmpty(response):
                r_id = int(response[0][0])
                response = self.addItem(r_id)
                # UpdateOnly
                if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                    self.logger.error("Response to addItem has error")
                    return False
            else:
                self.logger.warning("Response to getRelated1 is empty")
            
        # 4. resetCartTime
        query = sql.replaceVars(sql.sqlNameToCommand["resetCartTime"], 1, [self.shopping_id])
        # EarlyRelease - shopping_cart
        ertables = ["shopping_cart"]
        response = self.send_query_and_receive_response(query, "resetCartTime", ertables)
        # UpdateOnly:
        if not UPDATE_NO_ERR_CHECK and self.isErr(response):
            self.logger.error("Response to resetCartTime has error")
            return False

        # 5. getCart
        response = self.getCart()
        if self.isErr(response):
            self.logger.error("Response to getCart has error")
            return False
        

        # # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            self.logger.error("Response to getRelated has error")
            return False

        return True

    '''
    ==================================================================================================================
    ==================================================================================================================
    ==================================================================================================================
    '''

    def send_query_and_receive_response(self, query, name, ertables=[]):
        # take raw query, return result in list -> result[row][col]
        pairs = sql.sqlNameToOP[name]
        ops = {"READ":set(), "WRITE":set()}
        for table in pairs:
            if pairs[table] == "W":
                ops["WRITE"].add(table)
            else:
                ops["READ"].add(table)

        readString = ""
        writeString = ""
        if len(ops["READ"]):
            readString = "READ " + " ".join(ops["READ"])
        if len(ops["WRITE"]):
            writeString = "WRITE " + " ".join(ops["WRITE"])
        opsString = readString + " " + writeString


        serialized = json.dumps({
            "request_msql_text":{
                "op":"query",
                "query":query,
                "tableops":opsString,
                "ertables":" ".join(ertables)
            }
        })
        
        self.logger.info("### Sending data: QUERY {}".format(name))
        self.logger.info(query)
        self.logger.debug(serialized)

        self.soc.sendall(jsonToByte(serialized))
        try:
            response = byteToJson(self.soc.recv(2**24))
        
            self.logger.info("### Receiving data: Query {}".format(name))
            self.logger.debug(response)

            if MOCK:
                self.logger.warning("mock_db mode on")
                return ["0"] * 20 

            if ALLOW_ABORT and OK not in response["reply"]["Query"]:
                if "aborted" in response["reply"]["Query"]["Err"]:
                    return "Abort"

            if OK not in response["reply"]["Query"]:
                self.logger.error("Response to {} contains error".format(name))
                return "Err"

            response = response["reply"]["Query"][OK]
            self.logger.critical("csv string: {}".format(response))
            if not response:
                self.logger.warning("Response to {} is empty".format(name))
                return "Empty"

            result = list(csv.reader(response.splitlines()))
            for i in range(len(result)):
                for j in range(len(result[0])):
                    if result[i][j].startswith('"') and result[i][j].endswith('"'):
                        result[i][j] = result[i][j][1:-1]
            self.logger.info("csv list: {}".format(result))
            
            return result
        
        except:
            print("query tcp receiving failed")
            return "Abort"
    
    def isErr(self, response):
        return response == "Err"
    
    def isEmpty(self, response):
        return response == "Empty"

    def isAbort(self, response):
        return response == "Abort"

    def processCart(self, response, discount):
        cart = {"lines":[], "sc_sub_total":0, "sc_total":0, "total_items":0}
        # process each cart line
        for i in range(len(response)):
            row = response[i]
            cart["lines"].append({})
            cart["lines"][i]["scl_i_id"] = row[2] 
            cart["lines"][i]["scl_qty"] = int(row[1])
            cart["lines"][i]["i_cost"] = float(row[18])
            cart["total_items"] = cart["total_items"] + cart["lines"][i]["scl_qty"]
            cart["sc_sub_total"] = cart["sc_sub_total"] + cart["lines"][i]["scl_qty"] * cart["lines"][i]["i_cost"]

        # apply discount
        # subtotal is after discount, but before tax and shipping
        # total is total
        cart["sc_sub_total"] = cart["sc_sub_total"] * (100 - discount)*0.01
        tax = cart["sc_sub_total"] * 0.0825
        shipping = 3.00 + 1.00 * cart["total_items"]
        cart["sc_total"] = cart["sc_sub_total"] + tax + shipping 
        return cart

    '''
    ==================================================================================================================
    ==================================================================================================================
    ==================================================================================================================
    '''

    # All sql handler return the last response (might an intermediate Err response if a sequence)

    def getRelated(self):
        # getRelated - generate a random i_id (item id) as argument
        i_id = randint(1, NUM_ITEM)
        query = sql.replaceVars(sql.sqlNameToCommand["getRelated"], 1, [i_id])
        response = self.send_query_and_receive_response(query, "getRelated")
        # DispOnly
        return response

    def addItem(self, i_id):
        # addItem (sequence) 
        #    a. addItem
        #    b. addItemUpdate (if result not empty) or addItemPut (if result empty)

        # if no valid i_id given, generate a random value
        if i_id == -1:
            i_id = randint(1, NUM_ITEM)
        query = sql.replaceVars(sql.sqlNameToCommand["addItem"], 2, [self.shopping_id, i_id])
        response = self.send_query_and_receive_response(query, "addItem")
        # ReadResponse - read SELECT scl_qty
        if self.isAbort(response):
            self.logger.warning("Response to addItem is abort")
            return response
        if self.isErr(response):
            self.logger.error("Response to addItem has error")
            return response
        if self.isEmpty(response):
            self.logger.warning("Response to addItem is empty")
            # addItemPut
            query = sql.replaceVars(sql.sqlNameToCommand["addItemPut"], 3, [self.shopping_id, 1, i_id])
            response = self.send_query_and_receive_response(query, "addItemPut")
            # UpdateOnly
            if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                self.logger.error("Response to addItemPut has error")
                return response
        else:
            # addItemUpdate
            newQty = int(response[0][0]) + 1
            query = sql.replaceVars(sql.sqlNameToCommand["addItemUpdate"], 3, [newQty, self.shopping_id, i_id])
            response = self.send_query_and_receive_response(query, "addItemUpdate")
            # UpdateOnly
            if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                self.logger.error("Response to addItemUpdate has error")
                return response
        
        return response

    def getCart(self):
        # getCart
        query = sql.replaceVars(sql.sqlNameToCommand["getCart"], 1, [self.shopping_id])
        # EarlyRelease - shopping_cart_line; if self.curr == shopCart
        ertables = []
        if self.curr == "shopCart":
            ertables = ["shopping_cart_line"]
        response = self.send_query_and_receive_response(query, "getCart", ertables)
        # DispOnly
        return response

    def getBook(self, i_id):
        # getBook
        if i_id == -1:
            # If invalid i_id, generate a random one
            i_id = randint(1, NUM_ITEM)
        query = sql.replaceVars(sql.sqlNameToCommand["getBook"], 1, [i_id])
        # EarlyRelease - author; if self.curr == adminConf
        ertables = []
        if self.curr == "adminConf":
            ertables = ["author"]
        response = self.send_query_and_receive_response(query, "getBook", ertables)
        # DispOnly
        return response

    def enterAddress(self, street1, street2, city, state, zzip, country):
        addr_id = 0
        # enterAddress (sequence)
        #   a. enterAddressId
        query = sql.replaceVars(sql.sqlNameToCommand["enterAddressId"], 1, [country])
        # EarlyRelease - country; if self.curr == buyReq
        ertables = []
        if self.curr == "buyReq":
            ertables = ["country"]
        response = self.send_query_and_receive_response(query, "enterAddressId", ertables)
        # ReadResponse - SELECT co_id
        if self.isAbort(response):
            self.logger.warning("Response to enterAddressId is abort")
            return response
        if self.isErr(response):
            self.logger.error("Response to enterAddressId has error")
            return response
        if not self.isEmpty(response):
            co_id = response[0][0]
            #   b. enterAddressMatch
            query = sql.replaceVars(sql.sqlNameToCommand["enterAddressMatch"], 6, [street1, street2, city, state, zzip, co_id])
            response = self.send_query_and_receive_response(query, "enterAddressMatch")
            # ReadResponse - SELECT addr_id
            if self.isAbort(response):
                self.logger.warning("Response to enterAddressMatch is abort")
                return response
            if self.isErr(response):
                self.logger.error("Response to enterAddressMatch has error")
                return response
            if self.isEmpty(response):
                self.logger.warning("Response to enterAddressMatch is empty")
                #   c. enterAddressMaxId
                query = sql.sqlNameToCommand["enterAddressMaxId"]
                response = self.send_query_and_receive_response(query, "enterAddressMaxId")
                # ReadResponse - SELECT max(addr_id)
                if self.isAbort(response):
                    self.logger.warning("Response to enterAddressMaxId is abort")
                    return response
                if self.isErr(response):
                    self.logger.error("Response to enterAddressMaxId has error")
                    return response
                if self.isEmpty(response):
                    self.logger.warning("Response to enterAddressMaxId is empty")
                    return False # max has to have a number
                addr_id = int(response[0][0]) + 1

                #   d. enterAddressInsert
                query = sql.replaceVars(sql.sqlNameToCommand["enterAddressInsert"], 7, [addr_id, street1, street2, city, state, zzip, co_id])
                # EarlyRelease - address; if self.curr == buyReq
                ertables = []
                if self.curr == "buyReq":
                    ertables = ["address"]
                response = self.send_query_and_receive_response(query, "enterAddressInsert", ertables)
                # UpdateOnly
                if not UPDATE_NO_ERR_CHECK and self.isErr(response):
                    self.logger.error("Response to enterAddressInsert has error")
                    return response

            else:
                addr_id = response[0][0]
        else:
            self.logger.warning("Response to enterAddressId is empty")

        return [[addr_id]]


if __name__ == "__main__":
    # use port 56728 for testing
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=1077)
    parser.add_argument("--c_id", type=int)
    parser.add_argument("--mix", type=int, default=0)
    parser.add_argument("--debug", type=int, default=0)
    parser.add_argument("--mock_db", type=int, default=0)
    parser.add_argument("--ssh", type=int, default=0)

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
        sys.exit()
    # Check mix dimension
    if len(mix) != len(mix[0]):
        print("Probability table is not square! Terminating...")
        sys.exit()

    DEBUG = args.debug
    MOCK = args.mock_db
    HOST = '127.0.0.1'
    if args.ssh: 
        HOST = '128.100.13.240'

    newClient = Client(int(args.c_id), int(args.port), mix)
    if newClient.run():
        sys.exit(0) # no Err in the time period
    else:
        sys.exit(1)
