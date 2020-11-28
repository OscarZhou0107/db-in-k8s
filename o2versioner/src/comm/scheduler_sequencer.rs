use crate::core::version_number::TxVN;
use crate::core::msql::MsqlBeginTx;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
    Invalid,
    RequestTxVN(MsqlBeginTx),
    ReplyTxVN(TxVN),
}
