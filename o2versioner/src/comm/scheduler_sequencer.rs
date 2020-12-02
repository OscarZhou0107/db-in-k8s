use crate::core::{MsqlBeginTx, TxVN};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Invalid,
    RequestTxVN(MsqlBeginTx),
    ReplyTxVN(TxVN),
}
