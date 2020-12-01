import sql
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
    sqls = urlSql[url]
    result = {}
    for name in sqls:
        ops = sql.sqlNameToOP[name]
        for table in ops:
            if table not in result:
                result[table] = ops[table]
            else:
                # only update if this is a write operation
                if ops[table] == "W":
                    result[table] = ops[table]
    # convert result into a string
    resultList = ["BEGIN"]
    for table in result:
        resultList.append(table)
        resultList.append(result[table])
    return " ".join(resultList)


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
    print(getBegin("bestSell"))
    '''
    l = []
    for i in urlSql:
        for j in urlSql[i]:
            l.append(j)
    print(len(set(l)))