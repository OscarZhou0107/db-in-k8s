#![allow(warnings)]
use crate::core::database_version::*;
use crate::core::transaction_version::*;
use std::collections::HashMap;
use std::net::SocketAddr;

pub struct ConnectionState {
    cur_txvn: Option<TxVN>,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self { cur_txvn: None }
    }
}

impl ConnectionState {
    pub fn current_txvn(&self) -> &Option<TxVN> {
        &self.cur_txvn
    }

    /// Panics if there is no `TxVN` in the state.
    pub fn take_current_txvn(&mut self) -> TxVN {
        self.cur_txvn
            .take()
            .expect("Expecting there is a TxVN in the ConnectionState")
    }

    /// Panics if there is already a `TxVN` in the state.
    pub fn insert_txvn(&mut self, new_txvn: TxVN) {
        let existing = self.cur_txvn.replace(new_txvn);
        assert!(
            existing.is_none(),
            "Expecting there is no TxVN in the ConnectionState when inserting a new TxVN"
        );
    }
}

pub struct DbVNManager(HashMap<SocketAddr, DbVN>);

impl Default for DbVNManager {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

/// Unit test for `ConnectionState`
#[cfg(test)]
mod tests_connection_state {
    use super::*;

    #[test]
    fn test_take_current_txvn() {
        let mut conn_state = ConnectionState::default();
        assert_eq!(*conn_state.current_txvn(), None);
        conn_state.insert_txvn(TxVN::default());
        assert_eq!(conn_state.take_current_txvn(), TxVN::default());
        assert_eq!(*conn_state.current_txvn(), None);
    }

    #[test]
    #[should_panic]
    fn test_take_current_txvn_panic() {
        let mut conn_state = ConnectionState::default();
        conn_state.take_current_txvn();
    }

    #[test]
    #[should_panic]
    fn test_insert_txvn_panic() {
        let mut conn_state = ConnectionState::default();
        conn_state.insert_txvn(TxVN::default());
        conn_state.insert_txvn(TxVN::default());
    }
}
