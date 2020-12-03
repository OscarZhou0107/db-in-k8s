use crate::core::{ClientMeta, MsqlBeginTx, TxVN};
use serde::{Deserialize, Serialize};

#[derive(Debug, strum::AsRefStr, Serialize, Deserialize)]
pub enum Message {
    Invalid,
    RequestTxVN(ClientMeta, MsqlBeginTx),
    ReplyTxVN(Option<TxVN>),
    RequestBlock,
    RequestUnblock,
    ReplyBlockUnblock(String),
}
