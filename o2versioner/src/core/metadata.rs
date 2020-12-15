use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;

/// Meta data regarding the current client session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientMeta {
    client_addr: SocketAddr,
    cur_txid: usize,
}

impl fmt::Display for ClientMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "(client={} txid={})", self.client_addr, self.cur_txid)
    }
}

impl ClientMeta {
    pub fn new(client_addr: SocketAddr) -> Self {
        Self {
            client_addr,
            cur_txid: 0,
        }
    }

    /// Get the `ClientMeta::client_addr`
    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    /// Get the `ClientMeta::cur_txid`
    pub fn current_txid(&self) -> usize {
        self.cur_txid
    }

    /// Marks the current transaction as completed by
    /// incrementing `ClientMeta::cur_txid`
    pub fn transaction_finished(&mut self) {
        self.cur_txid += 1;
    }
}

/// Meta data regarding the current request within the current client session
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct RequestMeta {
    pub client_addr: SocketAddr,
    pub cur_txid: usize,
    pub request_id: usize,
}

impl fmt::Display for RequestMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "(client={} txid={} reqid={})",
            self.client_addr, self.cur_txid, self.request_id
        )
    }
}

impl RequestMeta {
    pub fn new(client_meta: &ClientMeta, request_id: usize) -> Self {
        Self {
            client_addr: client_meta.client_addr(),
            cur_txid: client_meta.current_txid(),
            request_id,
        }
    }

    /// Acquire a `ClientMeta` from the current `RequestMeta`
    pub fn to_client_meta(&self) -> ClientMeta {
        ClientMeta {
            client_addr: self.client_addr.clone(),
            cur_txid: self.cur_txid,
        }
    }
}

/// Unit test for `ClientMeta`
#[cfg(test)]
mod tests_client_meta {
    use super::*;

    #[test]
    fn test_transaction_finished() {
        let mut client_meta = ClientMeta::new("127.0.0.1:6666".parse().unwrap());
        assert_eq!(0, client_meta.current_txid());
        client_meta.transaction_finished();
        assert_eq!(1, client_meta.current_txid());
    }
}
