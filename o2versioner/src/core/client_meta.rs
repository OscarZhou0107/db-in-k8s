use std::net::SocketAddr;

#[derive(Debug)]
pub struct ClientMeta {
    client_addr: SocketAddr,
    cur_txid: usize,
}

impl ClientMeta {
    pub fn new(client_addr: SocketAddr) -> Self {
        Self {
            client_addr,
            cur_txid: 0,
        }
    }

    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    pub fn current_txid(&self) -> usize {
        self.cur_txid
    }

    pub fn transaction_finished(&mut self) {
        self.cur_txid += 1;
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
