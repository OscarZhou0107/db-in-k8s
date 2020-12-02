/// This module contains everything about the response
/// to a Msql query.
use crate::core::Msql;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum MsqlResponse {
    BeginTx(Result<(), String>),
    Query(Result<String, String>),
    EndTx(Result<String, String>),
}

impl MsqlResponse {
    pub fn err<S: Into<String>>(err: S, msql: &Msql) -> Self {
        match msql {
            Msql::BeginTx(_) => Self::begintx_err(err),
            Msql::Query(_) => Self::query_err(err),
            Msql::EndTx(_) => Self::endtx_err(err),
        }
    }

    pub fn begintx_err<S: Into<String>>(err: S) -> Self {
        Self::BeginTx(Err(err.into()))
    }

    pub fn begintx_ok() -> Self {
        Self::BeginTx(Ok(()))
    }

    pub fn query_err<S: Into<String>>(err: S) -> Self {
        Self::Query(Err(err.into()))
    }

    pub fn query_ok<S: Into<String>>(ok: S) -> Self {
        Self::Query(Ok(ok.into()))
    }

    pub fn endtx_err<S: Into<String>>(err: S) -> Self {
        Self::EndTx(Err(err.into()))
    }

    pub fn endtx_ok<S: Into<String>>(ok: S) -> Self {
        Self::EndTx(Ok(ok.into()))
    }
}
