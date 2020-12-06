import sql
import json
urlSql = {
    "home": [ # hello
        "getName", 
        # promo
        "getRelated" 
        ], 
    "shopCart": [
        # a sequence: createEmptyCart
        "createEmptyCart", # execute only if client.shopping_id == null
        "createEmptyCartInsertV2",
        
        # another sequence: doCart
        # inner sequence: addItem
        "addItem", 
        "addItemUpdate",
        "addItemPut",

        # another inner sequence: refreshCart
        "refreshCartRemove",
        "refreshCartUpdate",

        # another inner sequnce: addRandomItemToCartIfNecessary
        "addRandomItemToCartIfNecessary",
        "getRelated1",
        # inner inner sequence: addItem
        "addItem", 
        "addItemUpdate",
        "addItemPut",

        "resetCartTime",
        "getCart",
        # sequence end

        # promo
        "getRelated"
        ],
    "orderInq": [], # ...nothing...?
    "orderDisp": [
        "getPassword",
        # need valid username and password 
        # a sequence: GetMostRecentOrder
        "getMostRecentOrderId",
        "getMostRecentOrderOrder",
        "getMostRecentOrderLines"
        ],
    "searchReq": [
        # promo
        "getRelated" 
        ],
    "searchResult": [
        # promo
        "getRelated",
        # 1 out of 3
        "doAuthorSearch",
        "doTitleSearch",
        "doSubjectSearch"
        ],
    "newProd": [
        # promo
        "getRelated",
        "getNewProducts"
        ],
    "bestSell": [
        # promo
        "getRelated",
        "getBestSellers"
        ],
    "prodDet": [
        "getBook"
        ],
    "custReg": [
        "getUserName"
        ],
    "buyReq": [
        "getCustomer",
        "refreshSession",
        # a sequnce: createNewCustomer
        "createNewCustomer",
        # inner sequence: enterAddress
        "enterAddressId", 
        "enterAddressMatch", 
        "enterAddressInsert", 
        "enterAddressMaxId", 
        "createNewCustomerMaxId",

        "getCart"
        ],
    "buyConf": [
        # a sequence: doBuyConfirm 
        #    -> two versions: customer input new data vs. use address on file
        "getCDiscount",
        "getCart",

        # Option 1 - inner sequence: enterAddress
        "enterAddressId", 
        "enterAddressMatch", 
        "enterAddressInsert", 
        "enterAddressMaxId", 
        # or Option 2
        "getCAddr",

        # another inner sequnce: enterOrder
        "getCAddrId", # same sql as getCAddr
        "enterOrderMaxId",
        "enterOrderInsert",
        # loop based on how many lines there are in cart (returned by getCart)
        "addOrderLine",
        "getStock",
        "setStock",

        "enterCCXact",
        "clearCart"
        ],
    "adminReq": [
        "getBook"
        ],
    "adminConf": [
        "getBook",
        # a sequence: adminUpdate
        "adminUpdate",
        "adminUpdateRelated",
        "adminUpdateRelated1"
        ]
    # hello - getName. promo - getRelated
}

def getBegin(url):
    # find all sqls associated with a page
    sqls = urlSql[url]

    # result contains (table:op) pairs
    result = {}
    for name in sqls:
        # find all (table:op) pairs associated with an sql 
        # pairs[table] gives the op
        pairs = sql.sqlNameToOP[name]
        for table in pairs:
            if table not in result:
                result[table] = pairs[table]
            else:
                # only update if this is a write operation
                if pairs[table] == "W":
                    result[table] = pairs[table]

    # revResult contains (op:table) pairs
    revResult = {"READ":set(), "WRITE":set()}
    for table in result:
        if result[table] == "R":
            revResult["READ"].add(table)
        else:
            revResult["WRITE"].add(table)

    # convert result into a string
    readString = ""
    writeString = ""
    if len(revResult["READ"]):
        readString = "READ " + " ".join(revResult["READ"])
    if len(revResult["WRITE"]):
        writeString = "WRITE " + " ".join(revResult["WRITE"])
    resultString = readString + " " + writeString + "" 

    # serialize begin
    serialized = json.dumps({
        "request_msql_text":
        {
            "op":"begin_tx",
            "tableops":resultString
        }
    })
    return serialized

def getCommit():
    serialized = json.dumps({
        "request_msql_text":
        {
            "op":"end_tx",
            "mode":"commit"
        }
    })
    return serialized

def getCrash(curr):
    serialized = json.dumps({"request_crash":"fail in state ".format(curr)})
    return serialized

if __name__ == "__main__":
    '''
    l = []
    for i in urlSql:
        for j in urlSql[i]:
            l.append(j)
    ls = set(l)
    print(ls)
    print(len(ls))
    for i in sql.sqlName:
        if i not in ls:
            print(i)
    '''

    '''
    print(getBegin("home"))
    print(getBegin("buyConf"))
    '''

    '''
    l = []
    for i in urlSql:
        for j in urlSql[i]:
            l.append(j)
    print(len(set(l)))
    '''

    earlyRelease = {}
    for url in urlSql:
        earlyRelease[url] = []
        for name in urlSql[url]:
            earlyRelease[url].append([name, []])
    
    for url in urlSql:
        print(url)
        print(getBegin(url))
        numName = len(urlSql[url])
        for i in range(numName): # how many sqls are involved in each page
            name = urlSql[url][i]
            tables = sql.sqlNameToTable[name]
            for table in tables: # for each table this sql accesses, check if any later sqls access it
                                 # if there is no later sql, early release
                release = True
                for j in range(i+1, numName):
                    name2 = urlSql[url][j]
                    tables2 = sql.sqlNameToTable[name2]
                    if table in tables2:
                        release = False
                        break
                if i != numName - 1 and release:
                    if earlyRelease[url][i][0] != name:
                        print("wrong!")
                        sys.exit()
                    earlyRelease[url][i][1].append(table)
            if earlyRelease[url][i][1]:
                print("    {} {}".format(name, earlyRelease[url][i][1]))
            else:
                print("    {}".format(name))



'''
    newProd
    {"request_msql_text": {"tableops": "READ item author '", "op": "begin_tx"}}
        getRelated []
        getNewProducts []
    bestSell
    {"request_msql_text": {"tableops": "READ item orderline orders author '", "op": "begin_tx"}}
        getRelated []
        getBestSellers []
    searchResult
    {"request_msql_text": {"tableops": "READ item author '", "op": "begin_tx"}}
        getRelated []
        doAuthorSearch []
        doTitleSearch []
        doSubjectSearch []
    buyReq
    {"request_msql_text": {"tableops": "READ shopping_cart_line country item WRITE customer address'", "op": "begin_tx"}}
        getCustomer []
        refreshSession []
        createNewCustomer []
        enterAddressId ['country']
        enterAddressMatch []
        enterAddressInsert []
        enterAddressMaxId ['address']
        createNewCustomerMaxId ['customer']
        getCart []
    home
    {"request_msql_text": {"tableops": "READ customer item '", "op": "begin_tx"}}
        getName ['customer']
        getRelated []
    prodDet
    {"request_msql_text": {"tableops": "READ item author '", "op": "begin_tx"}}
        getBook []
    orderInq
    {"request_msql_text": {"tableops": " '", "op": "begin_tx"}}
    orderDisp
    {"request_msql_text": {"tableops": "READ customer order_line country item address cc_xacts orders '", "op": "begin_tx"}}
        getPassword []
        getMostRecentOrderId []
        getMostRecentOrderOrder ['customer', 'country', 'cc_xacts', 'orders', 'address']
        getMostRecentOrderLines []
    custReg
    {"request_msql_text": {"tableops": "READ customer '", "op": "begin_tx"}}
        getUserName []
    searchReq
    {"request_msql_text": {"tableops": "READ item '", "op": "begin_tx"}}
        getRelated []
    buyConf
    {"request_msql_text": {"tableops": "READ customer country WRITE shopping_cart_line order_line item address cc_xacts orders'", "op": "begin_tx"}}
        getCDiscount []
        getCart []
        enterAddressId []
        enterAddressMatch []
        enterAddressInsert []
        enterAddressMaxId []
        getCAddr []
        getCAddrId ['customer']
        enterOrderMaxId []
        enterOrderInsert ['orders']
        addOrderLine ['order_line']
        getStock []
        setStock ['item']
        enterCCXact ['country', 'cc_xacts', 'address']
        clearCart []
    adminReq
    {"request_msql_text": {"tableops": "READ item author '", "op": "begin_tx"}}
        getBook []
    adminConf
    {"request_msql_text": {"tableops": "READ author orders order_line WRITE item'", "op": "begin_tx"}}
        getBook ['author']
        adminUpdate []
        adminUpdateRelated ['orders', 'order_line']
        adminUpdateRelated1 []
    shopCart
    {"request_msql_text": {"tableops": "READ item WRITE shopping_cart_line shopping_cart'", "op": "begin_tx"}}
        createEmptyCart []
        createEmptyCartInsertV2 []
        addItem []
        addItemUpdate []
        addItemPut []
        refreshCartRemove []
        refreshCartUpdate []
        addRandomItemToCartIfNecessary []
        getRelated1 []
        addItem []
        addItemUpdate []
        addItemPut []
        resetCartTime ['shopping_cart']
        getCart ['shopping_cart_line']
        getRelated []
'''