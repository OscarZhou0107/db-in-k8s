use crate::common::sql::{Operation};
use crate::common::version_number::{TxVN, VN};
use std::collections::HashMap;


// Schduler knows the VN for every operation

// A single operation within a transaction (non-BEGIN)
#[allow(dead_code)]
struct TransOp {
    scheduler: String, // there might be multiple scheduler; can use integer as well
    client: String, // the cookie indicating which client this operation is from
    sql: String, // the sql operation to send to db proxies
    tables: Vec<String>, // an operation can access multiple tables
    table_vns: HashMap<String, (VN, Operation)>, // each accessed table has a VN
}

#[allow(dead_code)]
impl TransOp {
    // read from TxVN (vector of TableVN {table (name), vn, op})
    fn assign_vn(&mut self, tx_vn: &TxVN) {
        // if elem's table name matches tables accessed by this transaction, 
        // insert it to table_vns
        for entry in tx_vn.table_vns.iter() {
            if self.tables.contains(&entry.table) {
                self.table_vns.insert(entry.table.clone(), (entry.vn, entry.op));
            }
        }
    }

    // populate client, sql and tables fields when receiving a request from a client, through appserver
    fn populate(&mut self) {

    }


}

// on a BEGIN, send TxTable to sequencer to get TxVN
// --> might be performed solely by appserver? maybe can use TxTable to populate TransOp

/*
Plan:
    Two Vectors:
        one storing requests from client that hasn't fowarded to db, 
        one storing responses from db that hasn't forwardded to clients
    In the main while loop, every loop iterate through both Vectors, to process some entries if possible

    Need to somehow keep track of which VN each table is currently on -> maybe need to ask db to broadcast?
*/

// on a READ

// on a WRITE

// on a COMMIT