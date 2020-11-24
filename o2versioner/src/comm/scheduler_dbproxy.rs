use crate::{core::sql::TxTable, dbproxy::core::{Operation, QueryResult}};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum Message {
    Invalid,
    SqlRequest(Operation),
    SqlResponse(QueryResult),
}
