use crate::common::sql::TxTable;
use crate::common::version_number::TxVN;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Invalid,
    TxVNRequest(TxTable),
    TxVNResponse(TxVN),
}

impl Default for Message {
    fn default() -> Self {
        Message::Invalid
    }
}
