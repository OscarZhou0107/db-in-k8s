use crate::core::sql::TxTable;
use crate::core::version_number::TxVN;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    TxVNRequest(TxTable),
    TxVNResponse(TxVN),
}
