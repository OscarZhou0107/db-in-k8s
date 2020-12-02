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
    resultString = "BEGIN TRAN WITH MARK '" + readString + " " + writeString + "'" 

    # serialize
    serialized = json.dumps({
        "request_msql_text":{
        "op":"begin_tx",
        "tableops":resultString
        }
    })
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
    print(getBegin("home"))
    print(getBegin("buyConf"))
    '''
    l = []
    for i in urlSql:
        for j in urlSql[i]:
            l.append(j)
    print(len(set(l)))
    '''