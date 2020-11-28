



sql_name = [
    'getName', 'getBook', 'getCustomer', 'doSubjectSearc', 
    'doTitleSearch', 'doAuthorSearch', 'getNewProducts', 'getBestSellers', 

    'getRelated', 'adminUpdate', 'adminUpdateRelated', 'adminUpdate.related1', 
    'getUserName', 'getPassword', 'getRelated1', 'getMostRecentOrder.id', 
    'getMostRecentOrder.order', 'getMostRecentOrder.lines', 'createEmptyCart', 'createEmptyCart.insert.v2', 
    'addItem', 'addItem.update', 'addItem.put', 'refreshCart.remove', 
    'refreshCart.update', 'addRandomItemToCartIfNecessary', 'resetCartTime', 'getCart', 
    'refreshSession', 'createNewCustomer', 'createNewCustomer.maxId', 'getCDiscount', 
    'getCAddrId', 'getCAddr', 'enterCCXact', 'clearCart', 
    'enterAddress.id', 'enterAddress.match', 'enterAddress.insert', 'enterAddress.maxId', 
    'enterOrder.insert', 'enterOrder.maxId', 'addOrderLine', 'getStock', 
    'setStock', 'verifyDBConsistency.custId', 'verifyDBConsistency.itemId', 'verifyDBConsistency.addrId'
]

sql_command = [
    "SELECT c_fname,c_lname FROM customer WHERE c_id = ?",
    "SELECT * FROM item,author WHERE item.i_a_id = author.a_id AND i_id = ?",
    "SELECT * FROM customer, address, country WHERE customer.c_addr_id = address.addr_id AND address.addr_co_id = country.co_id AND customer.c_uname = ?",
    "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND item.i_subject = ? ORDER BY item.i_title limit 50",

    "SELECT * FROM item, author WHERE item.i_a_id = author.a_id AND substring(soundex(item.i_title),1,4)=substring(soundex(?),1,4) ORDER BY item.i_title limit 50",
    "SELECT * FROM author, item WHERE substring(soundex(author.a_lname),1,4)=substring(soundex(?),1,4) AND item.i_a_id = author.a_id ORDER BY item.i_title limit 50",
    '''
    SELECT i_id, i_title, a_fname, a_lname 
		 FROM item, author 
		 WHERE item.i_a_id = author.a_id 
		 AND item.i_subject = ? 
		 ORDER BY item.i_pub_date DESC,item.i_title 
		 limit 50"
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
    '''

]