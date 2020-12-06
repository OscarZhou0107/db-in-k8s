#![allow(dead_code)]
use crate::comm::MsqlResponse;
use crate::core::*;
use chrono::{DateTime, Utc};
use std::net::SocketAddr;

pub struct RequestRecordStart {
    req: Msql,
    req_timestamp: DateTime<Utc>,
    initial_txvn: Option<TxVN>,
}

impl RequestRecordStart {
    pub fn finish(self, res: &MsqlResponse, final_txvn: &Option<TxVN>) -> RequestRecord {
        let RequestRecordStart {
            req,
            req_timestamp,
            initial_txvn,
        } = self;

        assert!(
            (req.is_begintx() && res.is_begintx())
                || (req.is_query() && res.is_query())
                || (req.is_endtx() && res.is_endtx()),
            "Request must match with Response type"
        );

        RequestRecord {
            req,
            req_timestamp,
            initial_txvn,
            res: res.clone(),
            res_timestamp: Utc::now(),
            final_txvn: final_txvn.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestRecord {
    req: Msql,
    req_timestamp: DateTime<Utc>,
    initial_txvn: Option<TxVN>,
    res: MsqlResponse,
    res_timestamp: DateTime<Utc>,
    final_txvn: Option<TxVN>,
}

impl RequestRecord {
    /// Construct a builder object for `RequestRecord`.
    /// Once the builder `RequestRecordStart` is constructed, it will create a initial timestamp.
    /// Once the operation finishes and all data for the `RequestRecord` is ready,
    /// the builder object can be converted into the final `RequestRecord` by `RequestRecordStart::finish`,
    /// which will create final timestamp.
    pub fn start(req: &Msql, initial_txvn: &Option<TxVN>) -> RequestRecordStart {
        RequestRecordStart {
            req: req.clone(),
            req_timestamp: Utc::now(),
            initial_txvn: initial_txvn.clone(),
        }
    }

    pub fn request_info(&self) -> (&Msql, &DateTime<Utc>) {
        (&self.req, &self.req_timestamp)
    }

    pub fn reponse_info(&self) -> (&MsqlResponse, &DateTime<Utc>) {
        (&self.res, &self.res_timestamp)
    }

    pub fn inital_txvn(&self) -> &Option<TxVN> {
        &self.initial_txvn
    }

    pub fn final_txvn(&self) -> &Option<TxVN> {
        &self.final_txvn
    }

    pub fn elapsed(&self) -> chrono::Duration {
        self.res_timestamp - self.req_timestamp
    }

    pub fn is_begintx(&self) -> bool {
        self.req.is_begintx()
    }

    pub fn is_query(&self) -> bool {
        self.req.is_query()
    }

    pub fn is_endtx(&self) -> bool {
        self.req.is_endtx()
    }
}

#[derive(Debug)]
pub struct ClientRecord {
    client_addr: SocketAddr,
    records: Vec<RequestRecord>,
}

impl ClientRecord {
    pub fn new(client_addr: SocketAddr) -> Self {
        Self {
            client_addr,
            records: Vec::new(),
        }
    }

    /// Get the client_addr that this `ClientRecord` is tracking
    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr.clone()
    }

    /// Get the slices for all `RequestRecord`
    pub fn records(&self) -> &[RequestRecord] {
        &self.records
    }

    /// Append a new `RequestRecord` to the end of the list
    pub fn push(&mut self, req_record: RequestRecord) {
        self.records.push(req_record)
    }

    /// Returns a `Vec<PerformanceRequestRecord>` that is converted from
    /// all `RequestRecord` of the current `client_addr`
    pub fn get_performance_records(&self) -> Vec<PerformanceRequestRecord> {
        self.records.iter().cloned().map(|reqrecord| reqrecord.into()).collect()
    }
}

/// For performance benchmarking, converted from `RequestRecord`
#[derive(Debug)]
pub struct PerformanceRequestRecord {
    request_type: String,
    request_result: String,
    initial_timestamp: DateTime<Utc>,
    final_timestamp: DateTime<Utc>,
}

impl From<RequestRecord> for PerformanceRequestRecord {
    fn from(r: RequestRecord) -> Self {
        let request_type = match &r.req {
            Msql::BeginTx(_) => "BeginTx".to_owned(),
            Msql::Query(query) => {
                if r.initial_txvn.is_some() {
                    if !query.has_early_release() {
                        query.tableops().access_pattern().as_ref().to_owned()
                    } else {
                        format!("{}EarlyRelease", query.tableops().access_pattern().as_ref())
                    }
                } else {
                    format!("Single{}", query.tableops().access_pattern().as_ref())
                }
            }
            Msql::EndTx(endtx) => endtx.mode().as_ref().to_owned(),
        };

        let request_result = if r.res.is_ok() { "Ok" } else { "Err" };

        Self {
            request_type,
            request_result: request_result.into(),
            initial_timestamp: r.req_timestamp,
            final_timestamp: r.res_timestamp,
        }
    }
}
