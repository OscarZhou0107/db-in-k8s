use crate::core::sql::SqlBeginTx;
use crate::core::version_number::TxVN;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Invalid,
    TxVNRequest(SqlBeginTx),
    TxVNResponse(TxVN),
}
