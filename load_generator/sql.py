



def replaceVars(s, num, vars):
    if num == 0:
        return s
    sl = list(s)
    if num != len(vars):
        print("wrong number of vars - Check!")
    ind = -1
    for i in range(num):
        ind = s.index("?", ind+1)
        sl[ind] = str(vars[i])
    return "".join(sl)


sqlName = [
    #1
    'getName', 'getBook', 'getCustomer', 'doSubjectSearch', 
    #2
    'doTitleSearch', 'doAuthorSearch', 'getNewProducts', 'getBestSellers', 
    #3
    'getRelated', 'adminUpdate', 'adminUpdateRelated', 'adminUpdateRelated1', 
    #4
    'getUserName', 'getPassword', 'getRelated1', 'getMostRecentOrderId', 
    #5
    'getMostRecentOrderOrder', 'getMostRecentOrderLines', 'createEmptyCart', 'createEmptyCartInsertV2', 
    #6
    'addItem', 'addItemUpdate', 'addItemPut', 'refreshCartRemove', 
    #7
    'refreshCartUpdate', 'addRandomItemToCartIfNecessary', 'resetCartTime', 'getCart', 
    #8
    'refreshSession', 'createNewCustomer', 'createNewCustomerMaxId', 'getCDiscount', 
    #9
    'getCAddrId', 'getCAddr', 'enterCCXact', 'clearCart', 
    #10
    'enterAddressId', 'enterAddressMatch', 'enterAddressInsert', 'enterAddressMaxId', 
    #11
    'enterOrderInsert', 'enterOrderMaxId', 'addOrderLine', 'getStock', 
    #12
    'setStock', 'verifyDBConsistencyCustId', 'verifyDBConsistencyItemId', 'verifyDBConsistencyAddrId'
]

sqlCommand = [
    #1
    "SELECT c_fname,c_lname FROM customer WHERE c_id = ?",
    "SELECT * FROM item,author WHERE item.i_a_id = author.a_id AND i_id = ?",
    "SELECT * FROM customer, address, country WHERE customer.c_addr_id = address.addr_id AND address.addr_co_id = country.co_id AND customer.c_uname = ?",
    "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND item.i_subject = ? ORDER BY item.i_title limit 50",

    #2
    "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND substring(soundex(item.i_title),1,4)=substring(soundex(?),1,4) ORDER BY item.i_title limit 50",
    "SELECT * FROM author, item WHERE substring(soundex(author.a_lname),1,4)=substring(soundex(?),1,4) AND item.i_a_id = author.a_id ORDER BY item.i_title limit 50",
    '''
    SELECT i_id, i_title, a_fname, a_lname 
		 FROM item, author 
		 WHERE item.i_a_id = author.a_id 
		 AND item.i_subject = ? 
		 ORDER BY item.i_pub_date DESC,item.i_title 
		 limit 50
    ''',
    '''
    SELECT i_id, i_title, a_fname, a_lname 
		 FROM item, author, order_line 
		 WHERE item.i_id = order_line.ol_i_id 
		 AND item.i_a_id = author.a_id 
		 AND order_line.ol_o_id > (SELECT MAX(o_id)-3333 FROM orders) 
		 AND item.i_subject = ? 
		 GROUP BY i_id, i_title, a_fname, a_lname 
		 ORDER BY SUM(ol_qty) DESC 
		 limit 50 
    ''',

    #3
    "SELECT J.i_id,J.i_thumbnail from item I, item J where (I.i_related1 = J.i_id or I.i_related2 = J.i_id or I.i_related3 = J.i_id or I.i_related4 = J.i_id or I.i_related5 = J.i_id) and I.i_id = ?",
    "UPDATE item SET i_cost = ?, i_image = ?, i_thumbnail = ?, i_pub_date = CURRENT_DATE WHERE i_id = ?",
    '''
    SELECT ol_i_id 
		 FROM orders, order_line 
		 WHERE orders.o_id = order_line.ol_o_id 
		 AND NOT (order_line.ol_i_id = ?) 
		 AND orders.o_c_id IN (SELECT o_c_id 
 		                       FROM orders, order_line 
		                       WHERE orders.o_id = order_line.ol_o_id 
		                       AND orders.o_id > (SELECT MAX(o_id)-10000 FROM orders) 
		                       AND order_line.ol_i_id = ?) 
		 GROUP BY ol_i_id 
		 ORDER BY SUM(ol_qty) DESC 
		 limit 5
    ''',
    "UPDATE item SET i_related1 = ?, i_related2 = ?, i_related3 = ?, i_related4 = ?, i_related5 = ? WHERE i_id = ?",

    #4
    "SELECT c_uname FROM customer WHERE c_id = ?",
    "SELECT c_passwd FROM customer WHERE c_uname = ?",
    "SELECT i_related1 FROM item where i_id = ?",
    '''
    SELECT o_id 
		     FROM customer, orders 
		     WHERE customer.c_id = orders.o_c_id 
		     AND c_uname = ? 
		     ORDER BY o_date, orders.o_id DESC 
		     limit 1 
    ''',

    #5
    '''
    "SELECT orders.*, customer.*, 
              cc_xacts.cx_type, 
              ship.addr_street1 AS ship_addr_street1, 
              ship.addr_street2 AS ship_addr_street2, 
              ship.addr_state AS ship_addr_state, 
              ship.addr_zip AS ship_addr_zip, 
              ship_co.co_name AS ship_co_name, 
              bill.addr_street1 AS bill_addr_street1, 
              bill.addr_street2 AS bill_addr_street2, 
              bill.addr_state AS bill_addr_state, 
              bill.addr_zip AS bill_addr_zip, 
              bill_co.co_name AS bill_co_name 
            FROM customer, orders, cc_xacts,
              address AS ship, 
              country AS ship_co, 
              address AS bill,  
              country AS bill_co 
            WHERE orders.o_id = ? 
              AND cx_o_id = orders.o_id 
              AND customer.c_id = orders.o_c_id 
              AND orders.o_bill_addr_id = bill.addr_id 
              AND bill.addr_co_id = bill_co.co_id 
              AND orders.o_ship_addr_id = ship.addr_id 
              AND ship.addr_co_id = ship_co.co_id 
              AND orders.o_c_id = customer.c_id
    ''',
    '''
    SELECT * 
		     FROM order_line, item 
		     WHERE ol_o_id = ? 
		     AND ol_i_id = i_id
    ''',
    "SELECT COUNT(*) FROM shopping_cart",
    '''
    INSERT into shopping_cart (sc_id, sc_time) 
		     VALUES (?, 
		     CURRENT_TIMESTAMP)
    ''',

    #6
    "SELECT scl_qty FROM shopping_cart_line WHERE scl_sc_id = ? AND scl_i_id = ?",
    "UPDATE shopping_cart_line SET scl_qty = ? WHERE scl_sc_id = ? AND scl_i_id = ?",
    "INSERT into shopping_cart_line (scl_sc_id, scl_qty, scl_i_id) VALUES (?,?,?)",
    "DELETE FROM shopping_cart_line WHERE scl_sc_id = ? AND scl_i_id = ?",

    #7
    "UPDATE shopping_cart_line SET scl_qty = ? WHERE scl_sc_id = ? AND scl_i_id = ?",
    "SELECT COUNT(*) from shopping_cart_line where scl_sc_id = ?",
    "UPDATE shopping_cart SET sc_time = CURRENT_TIMESTAMP WHERE sc_id = ?",
    '''
    SELECT * 
		 FROM shopping_cart_line, item 
		 WHERE scl_i_id = item.i_id AND scl_sc_id = ?
    ''',

    #8
    "UPDATE customer SET c_login = NOW(), c_expiration = (CURRENT_TIMESTAMP + INTERVAL '2 HOUR') WHERE c_id = ?",
    "INSERT into customer (c_id, c_uname, c_passwd, c_fname, c_lname, c_addr_id, c_phone, c_email, c_since, c_last_login, c_login, c_expiration, c_discount, c_balance, c_ytd_pmt, c_birthdate, c_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    "SELECT max(c_id) FROM customer",
    "SELECT c_discount FROM customer WHERE customer.c_id = ?",

    #9
    "SELECT c_addr_id FROM customer WHERE customer.c_id = ?",
    "SELECT c_addr_id FROM customer WHERE customer.c_id = ?",
    '''
    INSERT into cc_xacts (cx_o_id, cx_type, cx_num, cx_name, cx_expire, cx_xact_amt, cx_xact_date, cx_co_id) 
		 VALUES (?, ?, ?, ?, ?, ?, CURRENT_DATE, (SELECT co_id FROM address, country WHERE addr_id = ? AND addr_co_id = co_id)) 
    ''',
    "DELETE FROM shopping_cart_line WHERE scl_sc_id = ?",

    #10
    "SELECT co_id FROM country WHERE co_name = ?",
    '''
    SELECT addr_id FROM address 
		 WHERE addr_street1 = ? 
		 AND addr_street2 = ? 
		 AND addr_city = ? 
		 AND addr_state = ? 
		 AND addr_zip = ? 
		 AND addr_co_id = ? 
    ''',
    '''
    INSERT into address (addr_id, addr_street1, addr_street2, addr_city, addr_state, addr_zip, addr_co_id) 
		     VALUES (?, ?, ?, ?, ?, ?, ?) 
    ''',
    "SELECT max(addr_id) FROM address",

    #11
    '''
    INSERT into orders (o_id, o_c_id, o_date, o_sub_total, 
		 o_tax, o_total, o_ship_type, o_ship_date, 
		 o_bill_addr_id, o_ship_addr_id, o_status) 
		 VALUES (?, ?, CURRENT_DATE, ?, 8.25, ?, ?, CURRENT_DATE + INTERVAL '? DAY', ?, ?, 'Pending') 
    ''',
    "SELECT count(o_id) FROM orders",
    '''
    INSERT into order_line (ol_id, ol_o_id, ol_i_id, ol_qty, ol_discount, ol_comments) 
		 VALUES (?, ?, ?, ?, ?, ?) 
    ''',
    "SELECT i_stock FROM item WHERE i_id = ?",

    #12
    "UPDATE item SET i_stock = ? WHERE i_id = ?",
    "SELECT c_id FROM customer",
    "SELECT i_id FROM item",
    "SELECT addr_id FROM address"
]

sqlOP = [ # {table: R/W operation}
    #1
    {"customer":"R"},
    {"item":"R", "author":"R"},
    {"customer":"R", "address":"R", "country":"R"},
    {"item":"R", "author":"R"},
    #2
    {"item":"R", "author":"R"},
    {"item":"R", "author":"R"},
    {"item":"R", "author":"R"},
    {"item":"R", "author":"R", "order_line":"R"},
    #3
    {"item":"R"},
    {"item":"W"},
    {"orders":"R", "order_line":"R"},
    {"item":"W"},
    #4
    {"customer":"R"},
    {"customer":"R"},
    {"item":"R"},
    {"customer":"R", "orders":"R"},
    #5
    {"customer":"R", "orders":"R", "cc_xacts":"R", "address":"R", "country":"R"},
    {"order_line":"R", "item":"R"},
    {"shopping_cart":"R"},
    {"shopping_cart":"W"},
    #6
    {"shopping_cart_line":"R"},
    {"shopping_cart_line":"W"},
    {"shopping_cart_line":"W"},
    {"shopping_cart_line":"W"},
    #7
    {"shopping_cart_line":"W"},
    {"shopping_cart_line":"R"},
    {"shopping_cart":"W"},
    {"shopping_cart_line":"R", "item":"R"},
    #8
    {"customer":"W"},
    {"customer":"W"},
    {"customer":"R"},
    {"customer":"R"},
    #9
    {"customer":"R"},
    {"customer":"R"},
    {"cc_xacts":"W", "address":"W", "country":"W"},
    {"shopping_cart_line":"W"},
    #10
    {"country":"R"},
    {"address":"R"},
    {"address":"W"},
    {"address":"R"},
    #11
    {"orders":"W"},
    {"orders":"R"},
    {"order_line":"W"},
    {"R":"item"},
    #12
    {"item":"W"},
    {"customer":"R"},
    {"item":"R"},
    {"address":"R"},
]

sqlNameToOP = {'getName': {'customer': 'R'}, 'getBook': {'item': 'R', 'author': 'R'}, 'getCustomer': {'customer': 'R', 'address': 'R', 'country': 'R'}, 'doSubjectSearch': {'item': 'R', 'author': 'R'}, 'doTitleSearch': {'item': 'R', 'author': 'R'}, 'doAuthorSearch': {'item': 'R', 'author': 'R'}, 'getNewProducts': {'item': 'R', 'author': 'R'}, 'getBestSellers': {'item': 'R', 'author': 'R', 'order_line': 'R'}, 'getRelated': {'item': 'R'}, 'adminUpdate': {'item': 'W'}, 'adminUpdateRelated': {'orders': 'R', 'order_line': 'R'}, 'adminUpdateRelated1': {'item': 'W'}, 'getUserName': {'customer': 'R'}, 'getPassword': {'customer': 'R'}, 'getRelated1': {'item': 'R'}, 'getMostRecentOrderId': {'customer': 'R', 'orders': 'R'}, 'getMostRecentOrderOrder': {'customer': 'R', 'orders': 'R', 'cc_xacts': 'R', 'address': 'R', 'country': 'R'}, 'getMostRecentOrderLines': {'order_line': 'R', 'item': 'R'}, 'createEmptyCart': {'shopping_cart': 'R'}, 'createEmptyCartInsertV2': {'shopping_cart': 'W'}, 'addItem': {'shopping_cart_line': 'R'}, 'addItemUpdate': {'shopping_cart_line': 'W'}, 'addItemPut': {'shopping_cart_line': 'W'}, 'refreshCartRemove': {'shopping_cart_line': 'W'}, 'refreshCartUpdate': {'shopping_cart_line': 'W'}, 'addRandomItemToCartIfNecessary': {'shopping_cart_line': 'R'}, 'resetCartTime': {'shopping_cart': 'W'}, 'getCart': {'shopping_cart_line': 'R', 'item': 'R'}, 'refreshSession': {'customer': 'W'}, 'createNewCustomer': {'customer': 'W'}, 'createNewCustomerMaxId': {'customer': 'R'}, 'getCDiscount': {'customer': 'R'}, 'getCAddrId': {'customer': 'R'}, 'getCAddr': {'customer': 'R'}, 'enterCCXact': {'cc_xacts': 'W', 'address': 'W', 'country': 'W'}, 'clearCart': {'shopping_cart_line': 'W'}, 'enterAddressId': {'country': 'R'}, 'enterAddressMatch': {'address': 'R'}, 'enterAddressInsert': {'address': 'W'}, 'enterAddressMaxId': {'address': 'R'}, 'enterOrderInsert': {'orders': 'W'}, 'enterOrderMaxId': {'orders': 'R'}, 'addOrderLine': {'order_line': 'W'}, 'getStock': {'item':'R'}, 'setStock': {'item': 'W'}, 'verifyDBConsistencyCustId': {'customer': 'R'}, 'verifyDBConsistencyItemId': {'item': 'R'}, 'verifyDBConsistencyAddrId': {'address': 'R'}}
sqlNameToCommand = {'getName': 'SELECT c_fname,c_lname FROM customer WHERE c_id = ?', 'getBook': 'SELECT * FROM item,author WHERE item.i_a_id = author.a_id AND i_id = ?', 'getCustomer': 'SELECT * FROM customer, address, country WHERE customer.c_addr_id = address.addr_id AND address.addr_co_id = country.co_id AND customer.c_uname = ?', 'doSubjectSearch': 'SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND item.i_subject = ? ORDER BY item.i_title limit 10', 'doTitleSearch': 'SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND substring(soundex(item.i_title),1,4)=substring(soundex(?),1,4) ORDER BY item.i_title limit 50', 'doAuthorSearch': 'SELECT * FROM author, item WHERE substring(soundex(author.a_lname),1,4)=substring(soundex(?),1,4) AND item.i_a_id = author.a_id ORDER BY item.i_title limit 50', 'getNewProducts': '\n    SELECT i_id, i_title, a_fname, a_lname \n\t\t FROM item, author \n\t\t WHERE item.i_a_id = author.a_id \n\t\t AND item.i_subject = ? \n\t\t ORDER BY item.i_pub_date DESC,item.i_title \n\t\t limit 50\n    ', 'getBestSellers': '\n    SELECT i_id, i_title, a_fname, a_lname \n\t\t FROM item, author, order_line \n\t\t WHERE item.i_id = order_line.ol_i_id \n\t\t AND item.i_a_id = author.a_id \n\t\t AND order_line.ol_o_id > (SELECT MAX(o_id)-3333 FROM orders) \n\t\t AND item.i_subject = ? \n\t\t GROUP BY i_id, i_title, a_fname, a_lname \n\t\t ORDER BY SUM(ol_qty) DESC \n\t\t limit 50 \n    ', 'getRelated': 'SELECT J.i_id,J.i_thumbnail from item I, item J where (I.i_related1 = J.i_id or I.i_related2 = J.i_id or I.i_related3 = J.i_id or I.i_related4 = J.i_id or I.i_related5 = J.i_id) and I.i_id = ?', 'adminUpdate': 'UPDATE item SET i_cost = ?, i_image = ?, i_thumbnail = ?, i_pub_date = CURRENT_DATE WHERE i_id = ?', 'adminUpdateRelated': '\n    SELECT ol_i_id \n\t\t FROM orders, order_line \n\t\t WHERE orders.o_id = order_line.ol_o_id \n\t\t AND NOT (order_line.ol_i_id = ?) \n\t\t AND orders.o_c_id IN (SELECT o_c_id \n \t\t                       FROM orders, order_line \n\t\t                       WHERE orders.o_id = order_line.ol_o_id \n\t\t                       AND orders.o_id > (SELECT MAX(o_id)-10000 FROM orders) \n\t\t                       AND order_line.ol_i_id = ?) \n\t\t GROUP BY ol_i_id \n\t\t ORDER BY SUM(ol_qty) DESC \n\t\t limit 5\n    ', 'adminUpdateRelated1': 'UPDATE item SET i_related1 = ?, i_related2 = ?, i_related3 = ?, i_related4 = ?, i_related5 = ? WHERE i_id = ?', 'getUserName': 'SELECT c_uname FROM customer WHERE c_id = ?', 'getPassword': 'SELECT c_passwd FROM customer WHERE c_uname = ?', 'getRelated1': 'SELECT i_related1 FROM item where i_id = ?', 'getMostRecentOrderId': '\n    SELECT o_id \n\t\t     FROM customer, orders \n\t\t     WHERE customer.c_id = orders.o_c_id \n\t\t     AND c_uname = ? \n\t\t     ORDER BY o_date, orders.o_id DESC \n\t\t     limit 1 \n    ', 'getMostRecentOrderOrder': '\n    "SELECT orders.*, customer.*, \n              cc_xacts.cx_type, \n              ship.addr_street1 AS ship_addr_street1, \n              ship.addr_street2 AS ship_addr_street2, \n              ship.addr_state AS ship_addr_state, \n              ship.addr_zip AS ship_addr_zip, \n              ship_co.co_name AS ship_co_name, \n              bill.addr_street1 AS bill_addr_street1, \n              bill.addr_street2 AS bill_addr_street2, \n              bill.addr_state AS bill_addr_state, \n              bill.addr_zip AS bill_addr_zip, \n              bill_co.co_name AS bill_co_name \n            FROM customer, orders, cc_xacts,\n              address AS ship, \n              country AS ship_co, \n              address AS bill,  \n              country AS bill_co \n            WHERE orders.o_id = ? \n              AND cx_o_id = orders.o_id \n              AND customer.c_id = orders.o_c_id \n              AND orders.o_bill_addr_id = bill.addr_id \n              AND bill.addr_co_id = bill_co.co_id \n              AND orders.o_ship_addr_id = ship.addr_id \n              AND ship.addr_co_id = ship_co.co_id \n              AND orders.o_c_id = customer.c_id\n    ', 'getMostRecentOrderLines': '\n    SELECT * \n\t\t     FROM order_line, item \n\t\t     WHERE ol_o_id = ? \n\t\t     AND ol_i_id = i_id\n    ', 'createEmptyCart': 'SELECT COUNT(*) FROM shopping_cart', 'createEmptyCartInsertV2': '\n    INSERT into shopping_cart (sc_id, sc_time) \n\t\t     VALUES (?, \n\t\t     CURRENT_TIMESTAMP)\n    ', 'addItem': 'SELECT scl_qty FROM shopping_cart_line WHERE scl_sc_id = ? AND scl_i_id = ?', 'addItemUpdate': 'UPDATE shopping_cart_line SET scl_qty = ? WHERE scl_sc_id = ? AND scl_i_id = ?', 'addItemPut': 'INSERT into shopping_cart_line (scl_sc_id, scl_qty, scl_i_id) VALUES (?,?,?)', 'refreshCartRemove': 'DELETE FROM shopping_cart_line WHERE scl_sc_id = ? AND scl_i_id = ?', 'refreshCartUpdate': 'UPDATE shopping_cart_line SET scl_qty = ? WHERE scl_sc_id = ? AND scl_i_id = ?', 'addRandomItemToCartIfNecessary': 'SELECT COUNT(*) from shopping_cart_line where scl_sc_id = ?', 'resetCartTime': 'UPDATE shopping_cart SET sc_time = CURRENT_TIMESTAMP WHERE sc_id = ?', 'getCart': '\n    SELECT * \n\t\t FROM shopping_cart_line, item \n\t\t WHERE scl_i_id = item.i_id AND scl_sc_id = ?\n    ', 'refreshSession': "UPDATE customer SET c_login = NOW(), c_expiration = (CURRENT_TIMESTAMP + INTERVAL '2 HOUR') WHERE c_id = ?", 'createNewCustomer': 'INSERT into customer (c_id, c_uname, c_passwd, c_fname, c_lname, c_addr_id, c_phone, c_email, c_since, c_last_login, c_login, c_expiration, c_discount, c_balance, c_ytd_pmt, c_birthdate, c_data) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', 'createNewCustomerMaxId': 'SELECT max(c_id) FROM customer', 'getCDiscount': 'SELECT c_discount FROM customer WHERE customer.c_id = ?', 'getCAddrId': 'SELECT c_addr_id FROM customer WHERE customer.c_id = ?', 'getCAddr': 'SELECT c_addr_id FROM customer WHERE customer.c_id = ?', 'enterCCXact': '\n    INSERT into cc_xacts (cx_o_id, cx_type, cx_num, cx_name, cx_expire, cx_xact_amt, cx_xact_date, cx_co_id) \n\t\t VALUES (?, ?, ?, ?, ?, ?, CURRENT_DATE, (SELECT co_id FROM address, country WHERE addr_id = ? AND addr_co_id = co_id)) \n    ', 'clearCart': 'DELETE FROM shopping_cart_line WHERE scl_sc_id = ?', 'enterAddressId': 'SELECT co_id FROM country WHERE co_name = ?', 'enterAddressMatch': '\n    SELECT addr_id FROM address \n\t\t WHERE addr_street1 = ? \n\t\t AND addr_street2 = ? \n\t\t AND addr_city = ? \n\t\t AND addr_state = ? \n\t\t AND addr_zip = ? \n\t\t AND addr_co_id = ? \n    ', 'enterAddressInsert': '\n    INSERT into address (addr_id, addr_street1, addr_street2, addr_city, addr_state, addr_zip, addr_co_id) \n\t\t     VALUES (?, ?, ?, ?, ?, ?, ?) \n    ', 'enterAddressMaxId': 'SELECT max(addr_id) FROM address', 'enterOrderInsert': "\n    INSERT into orders (o_id, o_c_id, o_date, o_sub_total, \n\t\t o_tax, o_total, o_ship_type, o_ship_date, \n\t\t o_bill_addr_id, o_ship_addr_id, o_status) \n\t\t VALUES (?, ?, CURRENT_DATE, ?, 8.25, ?, ?, CURRENT_DATE + INTERVAL '? DAY', ?, ?, 'Pending') \n    ", 'enterOrderMaxId': 'SELECT count(o_id) FROM orders', 'addOrderLine': '\n    INSERT into order_line (ol_id, ol_o_id, ol_i_id, ol_qty, ol_discount, ol_comments) \n\t\t VALUES (?, ?, ?, ?, ?, ?) \n    ', 'getStock': 'SELECT i_stock FROM item WHERE i_id = ?', 'setStock': 'UPDATE item SET i_stock = ? WHERE i_id = ?', 'verifyDBConsistencyCustId': 'SELECT c_id FROM customer', 'verifyDBConsistencyItemId': 'SELECT i_id FROM item', 'verifyDBConsistencyAddrId': 'SELECT addr_id FROM address'}
sqlNameToTable = {'enterOrderMaxId': ['orders'], 'verifyDBConsistencyItemId': ['item'], 'getMostRecentOrderId': ['customer', 'orders'], 'clearCart': ['shopping_cart_line'], 'addItemPut': ['shopping_cart_line'], 'verifyDBConsistencyAddrId': ['address'], 'enterAddressInsert': ['address'], 'verifyDBConsistencyCustId': ['customer'], 'resetCartTime': ['shopping_cart'], 'enterAddressMaxId': ['address'], 'getBestSellers': ['item', 'order_line', 'orders', 'author'], 'setStock': ['item'], 'createEmptyCartInsertV2': ['shopping_cart'], 'adminUpdate': ['item'], 'addItemUpdate': ['shopping_cart_line'], 'enterCCXact': ['country', 'cc_xacts', 'address'], 'getRelated1': ['item'], 'enterAddressId': ['country'], 'refreshCartUpdate': ['shopping_cart_line'], 'addOrderLine': ['order_line'], 'getName': ['customer'], 'adminUpdateRelated1': ['item'], 'getCAddr': ['customer'], 'createNewCustomer': ['customer'], 'getStock': ['item'], 'getPassword': ['customer'], 'enterOrderInsert': ['orders'], 'doSubjectSearch': ['item', 'author'], 'getRelated': ['item'], 'enterAddressMatch': ['address'], 'addItem': ['shopping_cart_line'], 'getBook': ['item', 'author'], 'adminUpdateRelated': ['orders', 'order_line'], 'createNewCustomerMaxId': ['customer'], 'refreshCartRemove': ['shopping_cart_line'], 'getMostRecentOrderOrder': ['customer', 'country', 'cc_xacts', 'orders', 'address'], 'createEmptyCart': ['shopping_cart'], 'doTitleSearch': ['item', 'author'], 'getUserName': ['customer'], 'doAuthorSearch': ['item', 'author'], 'getNewProducts': ['item', 'author'], 'getMostRecentOrderLines': ['item', 'order_line'], 'getCDiscount': ['customer'], 'getCart': ['shopping_cart_line', 'item'], 'getCAddrId': ['customer'], 'addRandomItemToCartIfNecessary': ['shopping_cart_line'], 'refreshSession': ['customer'], 'getCustomer': ['customer', 'country', 'address']}


if __name__ == "__main__":
    '''
    test1 = "UPDATE item SET i_cost = ?, i_image = ?, i_thumbnail = ?, i_pub_date = CURRENT_DATE WHERE i_id = ?"
    print(replaceVars(test, 4, [1, 2, 3, 4]))
    '''
    '''
    for i in range(len(sqlName)):
        sqlNameToOP[sqlName[i]] = sqlOP[i]
    print(sqlNameToOP)
    print(len(sqlNameToOP))

    for i in range(len(sqlName)):
        sqlNameToCommand[sqlName[i]] = sqlCommand[i]
    print(sqlNameToCommand)
    print(len(sqlNameToCommand))
    '''
    '''
    res = []
    for i in sqlNameToOP:
        for j in sqlNameToOP[i]:
            #print(sqlNameToOP[i])
            if sqlNameToOP[i][j] == "W":
                print(i, sqlNameToOP[i])
                break
    '''
    pass

    '''
    newProd
    {"request_msql_text": {"tableops": "READ item author '", "op": "begin_tx"}}
        getRelated    :    ['item']
        getNewProducts    :    ['item', 'author']
    bestSell
    {"request_msql_text": {"tableops": "READ item order_line orders author '", "op": "begin_tx"}}
        getRelated    :    ['item']
        getBestSellers    :    ['item', 'order_line', 'orders', 'author']
    searchResult
    {"request_msql_text": {"tableops": "READ item author '", "op": "begin_tx"}}
        getRelated    :    ['item']
        doAuthorSearch    :    ['item', 'author']
        doTitleSearch    :    ['item', 'author']
        doSubjectSearch    :    ['item', 'author']
    buyReq
    {"request_msql_text": {"tableops": "READ shopping_cart_line country item WRITE customer address'", "op": "begin_tx"}}
        getCustomer    :    ['customer', 'country', 'address']
        refreshSession    :    ['customer']
        createNewCustomer    :    ['customer']
        enterAddressId    :    ['country']
        enterAddressMatch    :    ['address']
        enterAddressInsert    :    ['address']
        enterAddressMaxId    :    ['address']
        createNewCustomerMaxId    :    ['customer']
        getCart    :    ['shopping_cart_line', 'item']
    home
    {"request_msql_text": {"tableops": "READ customer item '", "op": "begin_tx"}}
        getName    :    ['customer']
        getRelated    :    ['item']
    prodDet
    {"request_msql_text": {"tableops": "READ item author '", "op": "begin_tx"}}
        getBook    :    ['item', 'author']
    orderInq
    {"request_msql_text": {"tableops": " '", "op": "begin_tx"}}
    orderDisp
    {"request_msql_text": {"tableops": "READ customer order_line country item address cc_xacts orders '", "op": "begin_tx"}}
        getPassword    :    ['customer']
        getMostRecentOrderId    :    ['customer', 'orders']
        getMostRecentOrderOrder    :    ['customer', 'country', 'cc_xacts', 'orders', 'address']
        getMostRecentOrderLines    :    ['item', 'order_line']
    custReg
    {"request_msql_text": {"tableops": "READ customer '", "op": "begin_tx"}}
        getUserName    :    ['customer']
    searchReq
    {"request_msql_text": {"tableops": "READ item '", "op": "begin_tx"}}
        getRelated    :    ['item']
    buyConf
    {"request_msql_text": {"tableops": "READ customer country WRITE shopping_cart_line order_line item address cc_xacts orders'", "op": "begin_tx"}}
        getCDiscount    :    ['customer']
        getCart    :    ['shopping_cart_line', 'item']
        enterAddressId    :    ['country']
        enterAddressMatch    :    ['address']
        enterAddressInsert    :    ['address']
        enterAddressMaxId    :    ['address']
        getCAddr    :    ['customer']
        getCAddrId    :    ['customer']
        enterOrderMaxId    :    ['orders']
        enterOrderInsert    :    ['orders']
        addOrderLine    :    ['order_line']
        getStock    :    ['item']
        setStock    :    ['item']
        enterCCXact    :    ['country', 'cc_xacts', 'address']
        clearCart    :    ['shopping_cart_line']
    adminReq
    {"request_msql_text": {"tableops": "READ item author '", "op": "begin_tx"}}
        getBook    :    ['item', 'author']
    adminConf
    {"request_msql_text": {"tableops": "READ author orders order_line WRITE item'", "op": "begin_tx"}}
        getBook    :    ['item', 'author']
        adminUpdate    :    ['item']
        adminUpdateRelated    :    ['orders', 'order_line']
        adminUpdateRelated1    :    ['item']
    shopCart
    {"request_msql_text": {"tableops": "READ item WRITE shopping_cart_line shopping_cart'", "op": "begin_tx"}}
        createEmptyCart    :    ['shopping_cart']
        createEmptyCartInsertV2    :    ['shopping_cart']
        addItem    :    ['shopping_cart_line']
        addItemUpdate    :    ['shopping_cart_line']
        addItemPut    :    ['shopping_cart_line']
        refreshCartRemove    :    ['shopping_cart_line']
        refreshCartUpdate    :    ['shopping_cart_line']
        addRandomItemToCartIfNecessary    :    ['shopping_cart_line']
        getRelated1    :    ['item']
        addItem    :    ['shopping_cart_line']
        addItemUpdate    :    ['shopping_cart_line']
        addItemPut    :    ['shopping_cart_line']
        resetCartTime    :    ['shopping_cart']
        getCart    :    ['shopping_cart_line', 'item']
        getRelated    :    ['item']


    '''

