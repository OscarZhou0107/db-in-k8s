#![allow(warnings)]
use crate::comm::MsqlResponse;
use crate::core::*;
use chrono::{DateTime, Utc};
use std::net::SocketAddr;

pub struct RequestRecordStart {
    req: Msql,
    req_timestamp: DateTime<Utc>,
}

impl RequestRecordStart {
    pub fn finish(self, res: &MsqlResponse) -> RequestRecord {
        let RequestRecordStart { req, req_timestamp } = self;

        RequestRecord {
            req,
            req_timestamp,
            res: res.clone(),
            res_timestamp: Utc::now(),
        }
    }
}

pub struct RequestRecord {
    req: Msql,
    req_timestamp: DateTime<Utc>,
    res: MsqlResponse,
    res_timestamp: DateTime<Utc>,
}

impl RequestRecord {
    pub fn start(req: &Msql) -> RequestRecordStart {
        RequestRecordStart {
            req: req.clone(),
            req_timestamp: Utc::now(),
        }
    }
}

pub struct ClientRecord {
    client_addr: SocketAddr,
    req_records: Vec<RequestRecord>,
}

impl ClientRecord {
    pub fn new(client_addr: SocketAddr) -> Self {
        Self {
            client_addr,
            req_records: Vec::new(),
        }
    }
}
