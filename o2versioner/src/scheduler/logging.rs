#![allow(dead_code)]
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

        assert!(
            (req.is_begintx() && res.is_begintx())
                || (req.is_query() && res.is_query())
                || (req.is_endtx() && res.is_endtx()),
            "Request must match with Response type"
        );

        RequestRecord {
            req,
            req_timestamp,
            res: res.clone(),
            res_timestamp: Utc::now(),
        }
    }
}

#[derive(Debug)]
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

    pub fn request_info(&self) -> (&Msql, &DateTime<Utc>) {
        (&self.req, &self.req_timestamp)
    }

    pub fn reponse_info(&self) -> (&MsqlResponse, &DateTime<Utc>) {
        (&self.res, &self.res_timestamp)
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

    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr.clone()
    }

    pub fn records(&self) -> &[RequestRecord] {
        &self.records
    }

    pub fn push(&mut self, req_record: RequestRecord) {
        self.records.push(req_record)
    }
}
