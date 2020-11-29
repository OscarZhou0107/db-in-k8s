

clientVar = ["c_id", "shopping_id", "name", "related_thumbnails", "related_item_ids", "item_id"]
sqlVar = {
    #1
    'getName': ["c_id"], # customer id, 
    'getBook': [], 
    'getCustomer': [], 
    'doSubjectSearc': [],
    #2
    'doTitleSearch': [], 
    'doAuthorSearch': [], 
    'getNewProducts': [], 
    'getBestSellers': [], 
    #3
    'getRelated': ["item_id"], # random number in the range of number of item in item table 
    'adminUpdate': [], 
    'adminUpdateRelated': [], 
    'adminUpdateRelated1': [], 
    #4
    'getUserName': [], 
    'getPassword': [], 
    'getRelated1': [], 
    'getMostRecentOrderId': [], 
    #5
    'getMostRecentOrderOrder': [], 
    'getMostRecentOrderLines': [], 
    'createEmptyCart': [], # no parameter required
    'createEmptyCartInsertV2': [], 
    #6
    'addItem': [], 
    'addItemUpdate': [], 
    'addItemPut': [], 
    'refreshCartRemove': [], 
    #7
    'refreshCartUpdate': [],
    'addRandomItemToCartIfNecessary': [], 
    'resetCartTime': [], 
    'getCart': [], 
    #8
    'refreshSession': [], 
    'createNewCustomer': [], 
    'createNewCustomerMaxId': [], 
    'getCDiscount': [], 
    #9
    'getCAddrId': [], 
    'getCAddr': [], 
    'enterCCXact': [], 
    'clearCart': [], 
    #10
    'enterAddressId': [], 
    'enterAddressMatch': [], 
    'enterAddressInsert': [], 
    'enterAddressMaxId': [], 
    #11
    'enterOrderInsert': [], 
    'enterOrderMaxId': [], 
    'addOrderLine': [], 
    'getStock': [], 
    #12
    'setStock': [], 
    'verifyDBConsistencyCustId': [], 
    'verifyDBConsistencyItemId': [], 
    'verifyDBConsistencyAddrId': []
}

sqlRes = {
    #1
    'getName': ["name"], # if can't find, set to unknown user 
    'getBook': [], 
    'getCustomer': [], 
    'doSubjectSearc': [],
    #2
    'doTitleSearch': [], 
    'doAuthorSearch': [], 
    'getNewProducts': [], 
    'getBestSellers': [], 
    #3
    'getRelated': ["related_thumbnails", "related_item_ids"], # both are vectors
    'adminUpdate': [], 
    'adminUpdateRelated': [], 
    'adminUpdateRelated1': [], 
    #4
    'getUserName': [], 
    'getPassword': [], 
    'getRelated1': [], 
    'getMostRecentOrderId': [], 
    #5
    'getMostRecentOrderOrder': [], 
    'getMostRecentOrderLines': [], 
    'createEmptyCart': ["shopping_id"], # return client shopping_id
    'createEmptyCartInsertV2': [], 
    #6
    'addItem': [], 
    'addItemUpdate': [], 
    'addItemPut': [], 
    'refreshCartRemove': [], 
    #7
    'refreshCartUpdate': [],
    'addRandomItemToCartIfNecessary': [], 
    'resetCartTime': [], 
    'getCart': [], 
    #8
    'refreshSession': [], 
    'createNewCustomer': [], 
    'createNewCustomerMaxId': [], 
    'getCDiscount': [], 
    #9
    'getCAddrId': [], 
    'getCAddr': [], 
    'enterCCXact': [], 
    'clearCart': [], 
    #10
    'enterAddressId': [], 
    'enterAddressMatch': [], 
    'enterAddressInsert': [], 
    'enterAddressMaxId': [], 
    #11
    'enterOrderInsert': [], 
    'enterOrderMaxId': [], 
    'addOrderLine': [], 
    'getStock': [], 
    #12
    'setStock': [], 
    'verifyDBConsistencyCustId': [], 
    'verifyDBConsistencyItemId': [], 
    'verifyDBConsistencyAddrId': []
}

def populateSqlVar(sqlVar):
    pass
    #sqlVar["init"]["home"]["sql"] = 
    #sqlVar["init"]["home"]["var"] = 


def init():
    sqlVar = {}
    for i in range(len(trans)):
        probMix1[trans[i]] = {}
        probMix2[trans[i]] = {}
        probMix3[trans[i]] = {}
        sqlVar[trans[i]] = {}
        for j in range(len(tras)):
            probMix1[trans[i]][trans[j]] = 0
            probMix2[trans[i]][trans[j]] = 0
            probMix3[trans[i]][trans[j]] = 0
            sqlVar[trans[i]][trans[j]] = {"sql":[], "var": []}
    return probMix1, probMix2, probMix3, sqlVar

def popProbMix(probMix, data):
    pass


if __name__ == "__main__":
    probMix1, probMix2, probMix3, sqlVar = init()

    sqlVar = populateSqlVar(sqlVar)