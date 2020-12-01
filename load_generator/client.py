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
NUM_QTY = 10
NUM_PAIR = 10
DEBUG = 1
WRONG_PASSWD_FRENQUENCY = 1 # out of 10
MAX_STRING_LEN = 100
MAX_NUM_LEN = 12

def determineNext(curr, prob):
    row = prob[curr]
    value = randint(0, MAX_PROB)
    for i in range(len(row)):
        if value < row[i]:
            return i

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
    return "".join(res)

def generateRandomNum():
    res = []
    allChar = "1234567890"
    for i in range(randint(1, MAX_NUM_LEN)):
        index = randint(0,len(allChar)-1)
        res.append(allChar[index])
    return int("".join(res))

def generateRandomSubject():
    subjects = ['ARTS', 'BIOGRAPHIES', 'BUSINESS', 'CHILDREN', 'COMPUTERS', 'COOKING', 
                'HEALTH', 'HISTORY', 'HOME', 'HUMOR', 'LITERATURE', 'MYSTERY', 'NON-FICTION', 
                'PARENTING', 'POLITICS', 'REFERENCE', 'RELIGION', 'ROMANCE', 'SCIENCE-FICTION', 
                'SCIENCE-NATURE', 'SELF-HELP', 'SPORTS', 'TRAVEL', 'YOUTH']
    index = randint(0, len(subjects)-1)
    return subjects[index]

def generateRandomCountry():
    countries = ['Algeria', 'Argentina', 'Armenia', 'Australia', 'Austria', 'Azerbaijan', 'Bahamas', 'Bahrain', 'Bangla Desh', 'Barbados', 'Belarus', 'Belgium', 'Bermuda', 'Bolivia', 'Botswana', 'Brazil', 'Bulgaria', 'Canada', 'Cayman Islands', 'Chad', 'Chile', 'China', 'Christmas Island', 'Colombia', 'Croatia', 'Cuba', 'Cyprus', 'Czech Republic', 'Denmark', 'Dominican Republic', 'Eastern Caribbean', 'Ecuador', 'Egypt', 'El Salvador', 'Estonia', 'Ethiopia', 'Falkland Island', 'Faroe Island', 'Fiji', 'Finland', 'France', 'Gabon', 'Germany', 'Gibraltar', 'Greece', 'Guam', 'Hong Kong', 'Hungary', 'Iceland', 'India', 'Indonesia', 'Iran', 'Iraq', 'Ireland', 'Israel', 'Italy', 'Jamaica', 'Japan', 'Jordan', 'Kazakhstan', 'Kuwait', 'Lebanon', 'Luxembourg', 'Malaysia', 'Mauritius', 'Mexico', 'Netherlands', 'New Zealand', 'Norway', 'Pakistan', 'Philippines', 'Poland', 'Portugal', 'Romania', 'Russia', 'Saudi Arabia', 'Singapore', 'Slovakia', 'South Africa', 'South Korea', 'Spain', 'Sudan', 'Sweden', 'Switzerland', 'Taiwan', 'Thailand', 'Trinidad', 'Turkey', 'United Kingdom', 'United States', 'Venezuela', 'Zambia']
    index = randint(0, len(countries)-1)
    return countries[index]

class Client:
    def _init_(self, c_id, port, mix):
        self.c_id = c_id
        self.port = port
        self.curr = "home"
        self.max_time = datetime.datetime.now() + datetime.timedelta(seconds=MAX_TIME)
        self.mix = mix
        self.new_session = True # will be updated in doHome
        self.load = False # will be updated in getName in doHome -> if True, existing customer with info loaded
        self.soc = None # will be updated once run() is called on a client
        self.shopping_id = None # will be updated in createEmptyCart in doShopCart
        self.c_uname = None # will be updated by getUserName in doCustReg, or by createNewCustomer in doBuyReq

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

'''
==================================================================================================================
==================================================================================================================
==================================================================================================================
'''

    # TODO: each response will be a JSON object through TCP
    # just use response["var_name"] to read back

    # Note: each function will take whatever retrieved by req.getParameter(varname) as argument 
    # sql response types:
    #   - DispOnly - SELECT, but no need to read data from response
    #   - UpdateOnly -> INSERT, DELETE, UPDATE...
    #   - ReadResponse -> need to read data from response
    

    def doAdminConf(self):
        return True

    def doAdminReq(self):
        return True

    def doBestSell(self):
        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            return False
        
        # getBestSellers
        subject = generateRandomSubject()
        query = sql.replaceVars(sql.sqlNameToCommand["getBestSellers"], 1, [subject])
        response = self.send_query_and_receive_response(query)
        # DispOnly
        if self.isErr(response):
            return False
        
        return True

    def doBuyConf(self):
        # getCDiscount
        query = sql.replaceVars(sql.sqlNameToCommand["getCDiscount"], 1, [self.c_id])
        response = self.send_query_and_receive_response(query)
        # DispOnly
        if self.isErr(response):
            return False

        # getCart
        if self.shopping_id:
            response = self.getCart()
            if self.isErr(response):
                return False
            processed = self.processCart(response) # handle all index and return a dictionary

            addr_id = 0
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
                if self.isErr(response):
                    return False
                if self.isEmpty(response):
                    return False
                addr_id = response[0][0]
            else:
                # getCAddr
                query = sql.replaceVars(sql.sqlNameToCommand["getCAddr"], 1, [self.c_id])
                response = self.send_query_and_receive_response(query)
                # ReadResponse - SELECT c_addr_id
                if self.isErr(response):
                    return False
                addr_id = response[0][0]

            # enterOrder (sequence)
            #   a. getCAddrId
            #   b. enterOrderMaxId
            #   c. enterOrderInsert

            # loop
            for row in processed:
                # addOrderLine
                # getStock
                # setStock

            # enterCCXact
            # clearCart

        return True

    def doBuyReq(self):
        flag = randint(0, 1)
        if (flag == 1): # only if flag is Y == 1
            if self.load: # only if both c_uname and c_passwd are given (implied by load)
                # getCustomer
                query = sql.replaceVars(sql.sqlNameToCommand["getCustomer"], 1, [self.c_uname])
                response = self.send_query_and_receive_response(query)
                # DispOnly
                if self.isErr(response):
                    return False

                # refreshSession 
                query = sql.replaceVars(sql.sqlNameToCommand["refreshSession"], 1, [self.c_id])
                response = self.send_query_and_receive_response(query)
                # UpdateOnly
                if self.isErr(response):
                    return False

                # use random number to simulate if input password is incorrect
                if randint(1, WRONG_PASSWD_FRENQUENCY) == 1:
                    return False
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
            if self.isErr(response):
                return False
            if self.isEmpty(response):
                return False
            addr_id = response[0][0]
           
            #   2. createNewCustomerMaxId
            query = sql.sqlNameToCommand["createNewCustomerMaxId"]
            response = self.send_query_and_receive_response(query)
            # ReadResponse - SELECT max(c_id)
            if self.isErr(response):
                return False
            max_id = response[0][0] + 1

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
            c_last_login = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            c_since = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            c_login = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            c_expiration = (datetime.datetime.now() + datetime.timedelta(hours=2)).strftime('%Y-%m-%d %H:%M:%S')
            c_discount = ranint(0, 50) * 1.0
            c_birthday = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            c_data = generateRandomString()
            cust_info = [   max_id, c_uname, c_passwd, c_fname, c_lname, 
                            addr_id, c_phone, c_email, c_since, c_last_login,
                            c_login, c_expiration, c_discount, 0.0, 0.0, 
                            c_birthdate, c_data
                        ]
            query = sql.replaceVars(sql.sqlNameToCommand["refreshSession"], 17, cust_info)
            response = self.send_query_and_receive_response(query)
            # UpdateOnly
            if self.isErr(response):
                return False

        # getCart
        if self.shopping_id:
            response = self.getCart()
            if self.isErr(response):
                return False

        return True

    def doCustReg(self):
        # getUserName
        query = sql.replaceVars(sql.sqlNameToCommand["getUserName"], 1, [self.c_id])
        response = self.send_query_and_receive_response(query)
        # ReadResponse - SELECT c_uname
        if self.isErr(response):
            return False
        self.c_uname = response[0][0]

        return True

    def doHome(self):
        # say hello - getName - c_id, shopping_id
        if self.new_session: # only getName when it is a new_session
            query = sql.replaceVars(sql.sqlNameToCommand["getName"], 1, [self.c_id])
            response = self.send_query_and_receive_response(query)
            # ReadResponse - if not empty, existing customer, load data
            if self.isErr(response):
                return False
            if not self.isEmpty(response):
                self.load = True
            self.new_session = False
 
        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            return False
        
        return True

    def doNewProd(self):
        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            return False

        # getNewProducts
        subject = generateRandomSubject()
        query = sql.replaceVars(sql.sqlNameToCommand["getNewProducts"], 1, [subject])
        response = self.send_query_and_receive_response(query)
            # DispOnly
            if self.isErr(response):
                return False

        return True

    def doOrderDisp(self):
        # 1. getPassword 
        if self.load: # only if both c_uname and c_passwd are given (implied by load)
            query = sql.replaceVars(sql.sqlNameToCommand["getPassword"], 1, [self.c_uname])
            response = self.send_query_and_receive_response(query)
            # DispOnly
            if self.isErr(response):
                return False
            
            # use random number to simulate if input password is correct
            if randint(1, WRONG_PASSWD_FRENQUENCY)> 1: 
                # 2. GetMostRecentOrder (sequence) - only execute if input password is correct
                #       a. getMostRecentOrderId
                #       b. getMostRecentOrderOrder - only execute if a. is not empty; need o_id from a.
                #       c. getMostRecentOrderLines - only execyte if b. is not empty; need o_id from a.
                    # GetMostRecentOrderId
                query = sql.replaceVars(sql.sqlNameToCommand["GetMostRecentOrderId"], 1, [self.c_uname])
                response = self.send_query_and_receive_response(query)
                # ReadResponse - SELECT o_id
                if self.isErr(response):
                    return False
                if not self.isEmpty(response):
                    o_id = response[0][0]
                    # getMostRecentOrderOrder
                    query = sql.replaceVars(sql.sqlNameToCommand["GetMostRecentOrderOrder"], 1, [o_id])
                    response = self.send_query_and_receive_response(query)
                    # ReadResponse - SELECT orders.*, customer.*; but only care if it is empty
                    if self.isErr(response):
                        return False
                    if not self.isEmpty(response):
                        query = sql.replaceVars(sql.sqlNameToCommand["GetMostRecentOrderLines"], 1, [o_id])
                        response = self.send_query_and_receive_response(query)
                        # DispOnly
                        if self.isErr(response):
                            return False


        return True

    def doOrderInq(self):
        return True

    def doProdDet(self):
        # getBook - generate random i_id
        i_id = randint(0, NUM_ITEM-1)
        query = sql.replaceVars(sql.sqlNameToCommand["getBook"], 1, [i_id])
        response = self.send_query_and_receive_response(query)
        # DispOnly
        if self.isErr(response):
            return False
        return True

    def doSearchReq(self):
        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            return False
        return True

    def doSearchResult(self):
        # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            return False
        
        # choose one of three searches to do -> use a random number to decide
        # doAuthorSearch
        # doTitleSearch
        # doSubjectSearch
        searchType = randint(1, 3)
        if searchType == 1:
            # author
            searchKey = generateRandomString()
            query = sql.replaceVars(sql.sqlNameToCommand["doAuthorSearch"], 1, [searchKey])
            response = self.send_query_and_receive_response(query)
            # DispOnly
            if self.isErr(response):
                return False
        elif searchType == 2:
            # title
            searchKey = generateRandomString()
            query = sql.replaceVars(sql.sqlNameToCommand["doTitleSearch"], 1, [searchKey])
            response = self.send_query_and_receive_response(query)
            # DispOnly
            if self.isErr(response):
                return False
        else: searchType == 3:
            # subject
            searchKey = generateRandomSubject()
            query = sql.replaceVars(sql.sqlNameToCommand["doSubjectSearch"], 1, [searchKey])
            response = self.send_query_and_receive_response(query)
            # DispOnly
            if self.isErr(response):
                return False

        return True

    def doShopCart(self):
        # createEmptyCart (sequence)
        if not self.shopping_id: # only createEmptyCart (sequence) if no shopping_id yet
            # 1. createEmptyCart
            query = sql.sqlNameToCommand["createEmptyCart"]
            response = self.send_query_and_receive_response(query)
            # ReadResponse - read COUNT
            if self.isErr(response):
                return False
            self.shopping_id = int(response[0][0])

            # 2. createEmptyCartInsertV2
            query = sql.replaceVars(sql.sqlNameToCommand["createEmptyCartInsertV2"], 1, [self.shopping_id])
            response = self.send_query_and_receive_response(query)
            # UpdateOnly:
            if self.isErr(response):
                return False

        # doCart (sequence)
        # 1. addItem (sequence) 
        #       - happens only when user set a flag, in which case only one i_id is given -> use random number
        flag = randint(0, 1)
        if flag:
            response = addItem(-1)
            if self.isErr(response):
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
                iid = randint(0, NUM_ITEM)
                if qty == 0:
                    query = sql.replaceVars(sql.sqlNameToCommand["refreshCartRemove"], 2, [self.shopping_id, iid])
                    response = self.send_query_and_receive_response(query)
                    # UpdateOnly
                    if self.isErr(response):
                        return False
                else:
                    query = sql.replaceVars(sql.sqlNameToCommand["refreshCartUpdate"], 3, [qty, self.shopping_id, iid])
                    response = self.send_query_and_receive_response(query)
                    # UpdateOnly
                    if self.isErr(response):
                        return False

        # 3. addRandomItemToCartIfNecessary (sequence)
        #       a. addRandomItemToCartIfNecessary
        #       b. getRelated1 - b.c. only if a. returned 0
        #       c. addItem
        query = sql.replaceVars(sql.sqlNameToCommand["addRandomItemToCartIfNecessary"], 1, [self.shopping_id])
        response = self.send_query_and_receive_response(query)
        # ReadResponse - read COUNT
        if self.isErr(response):
            return False
        count = int(response[0][0])

        if count == 0:
            i_id = randint(0, NUM_ITEM-1)
            query = sql.replaceVars(sql.sqlNameToCommand["getRelated1"], 1, [i_id])
            response = self.send_query_and_receive_response(query)
            # ReadResponse - read SELECT i_related1
            r_id = int(response[0][0])

            response = addItem(r_id)
            if self.isErr(response):
                return False
        
        # 4. resetCartTime
        query = sql.replaceVars(sql.sqlNameToCommand["resetCartTime"], 1, [self.shopping_id])
        response = self.send_query_and_receive_response(query)
        # UpdateOnly:
        if self.isErr(response):
            return False

        # 5. getCart
        response = self.getCart()
        if self.isErr(response):
            return False

        # # promo - getRelated
        response = self.getRelated()
        if self.isErr(response):
            return False

        return True
'''
==================================================================================================================
==================================================================================================================
==================================================================================================================
'''
    def send_query_and_receive_response(self, query):
        # take raw query, return result in list -> result[row][col]
        serialized = json_proc.construct_query(query)
        self.soc.sendall(serialized)
        response = self.soc.recv(2**24) # raw response
        # TODO: will get result in .csv, parse to list -> result[row][col]
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

'''
==================================================================================================================
==================================================================================================================
==================================================================================================================
'''

    # All sql handler return the last response (might an intermediate Err response if a sequence)

    def getRelated(self):
        # getRelated - generate a random i_id (item id) as argument
        i_id = randint(0, NUM_ITEM-1)
        query = sql.replaceVars(sql.sqlNameToCommand["getRelated"], 1, [i_id])
        response = self.send_query_and_receive_response(query)
        # DispOnly
        return response

    def addItem(self, i_id):
        # addItem (sequence) 
        #    a. addItem
        #    b. addItemUpdate (if result not empty) or addItemPut (if result empty)

        # if no valid i_id given, generate a random value
        if i_id == -1:
            i_id = randint(0, NUM_ITEM-1)
        query = sql.replaceVars(sql.sqlNameToCommand["addItem"], 2, [self.shopping_id, i_id])
        response = self.send_query_and_receive_response(query)
        # ReadResponse - read SELECT scl_qty
        if self.isErr(response):
            return response
        if self.isEmpty(response):
            # addItemPut
            query = sql.replaceVars(sql.sqlNameToCommand["addItemPut"], 3, [self.shopping_id, 1, i_id])
            response = self.send_query_and_receive_response(query)
            # UpdateOnly
            if self.isErr(response):
                return response
        else:
            # addItemUpdate
            newQty = response[0]["scl_qty"] + 1
            query = sql.replaceVars(sql.sqlNameToCommand["addItemUpdate"], 3, [newQty, self.shopping_id, i_id])
            response = self.send_query_and_receive_response(query)
            # UpdateOnly
            if self.isErr(response):
                return response
        
        return response

    def getCart(self):
        # getCart
        query = sql.replaceVars(sql.sqlNameToCommand["getCart"], 1, [self.shopping_id])
        response = self.send_query_and_receive_response(query)
        # DispOnly
        return response

    def enterAddress(self, street1, street2, city, state, zzip, country):
        addr_id = 0
        # enterAddress (sequence)
        #   a. enterAddressId
        query = sql.replaceVars(sql.sqlNameToCommand["enterAddressId"], 1, [country])
        response = self.send_query_and_receive_response(query)
        # ReadResponse - SELECT co_id
        if self.isErr(response):
            return response
        if not self.isEmpty(response):
            co_id = response[0][0]
            #   b. enterAddressMatch
            query = sql.replaceVars(sql.sqlNameToCommand["enterAddressMatch"], 6, [street1. street2, city, state, zzip, co_id])
            response = self.send_query_and_receive_response(query)
            # ReadResponse - SELECT addr_id
            if self.isErr(response):
                return response
            if self.isEmpty(response):
                #   c. enterAddressMaxId
                query = sql.sqlNameToCommand["enterAddressMaxId"]
                response = self.send_query_and_receive_response(query)
                # ReadResponse - SELECT max(addr_id)
                if self.isErr(response):
                    return response
                addr_id = response[0][0]

                #   d. enterAddressInsert
                query = sql.replaceVars(sql.sqlNameToCommand["enterAddressInsert"], 7, [addr_id, street1, street2, city, state, zzip, co_id])
                response = self.send_query_and_receive_response(query)
                # UpdateOnly
                if self.isErr(response):
                    return response

            else:
                addr_id = response[0][0]

        return [[addr_id]]


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
