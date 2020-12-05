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

    pub fn is_begintx(&self) -> bool {
        match self {
            Self::BeginTx(_) => true,
            _ => false,
        }
    }

    pub fn is_query(&self) -> bool {
        match self {
            Self::Query(_) => true,
            _ => false,
        }
    }

    pub fn is_endtx(&self) -> bool {
        match self {
            Self::EndTx(_) => true,
            _ => false,
        }
    }
}

/// Unit test for `MsqlResponse`
#[cfg(test)]
mod tests_msql_response {
    use super::*;
    use crate::core::*;

    #[test]
    fn test_err() {
        let msqlbegintx = Msql::BeginTx(MsqlBeginTx::default());
        let msqlquery = Msql::Query(MsqlQuery::new("", TableOps::default()).unwrap());
        let msqlendtx = Msql::EndTx(MsqlEndTx::commit());

        assert_eq!(MsqlResponse::err("a", &msqlbegintx), MsqlResponse::begintx_err("a"));
        assert_eq!(MsqlResponse::err("a", &msqlquery), MsqlResponse::query_err("a"));
        assert_eq!(MsqlResponse::err("a", &msqlendtx), MsqlResponse::endtx_err("a"));
    }

    #[test]
    fn test_ok() {
        assert!(MsqlResponse::begintx_ok().is_begintx());
        assert!(MsqlResponse::query_ok("a").is_query());
        assert!(MsqlResponse::endtx_ok("a").is_endtx());
    }
}
