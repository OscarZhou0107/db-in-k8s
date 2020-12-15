//! Communication between scheduler and dbproxy

use super::msql_response::MsqlResponse;
use crate::core::*;
use serde::{Deserialize, Serialize};

/// Expecting every request will have a response replied back via the same tcp stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// A `Msql` request to dbproxy. `Option<TxVN> == None` for single-read transaction
    MsqlRequest(RequestMeta, Msql, Option<TxVN>),
    /// The repsone to the `MsqlRequest`
    MsqlResponse(RequestMeta, MsqlResponse),
    /// Response to an invalid request, for exmample, sending `MsqlResponse(MsqlResponse)` to the dbproxy
    Invalid,
}

impl Message {
    pub fn try_get_request_meta(&self) -> Result<&RequestMeta, ()> {
        match self {
            Self::MsqlRequest(meta, _, _) => Ok(meta),
            Self::MsqlResponse(meta, _) => Ok(meta),
            _ => Err(()),
        }
    }
}
