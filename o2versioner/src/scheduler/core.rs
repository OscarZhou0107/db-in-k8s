#![allow(warnings)]
use crate::core::database_version::*;
use crate::core::operation::*;
use crate::core::transaction_version::*;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::net::SocketAddr;
use tracing::warn;

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

/// TODO: need unit testing
pub struct DbVNManager(HashMap<SocketAddr, DbVN>);

impl FromIterator<SocketAddr> for DbVNManager {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = SocketAddr>,
    {
        Self(iter.into_iter().map(|addr| (addr, DbVN::default())).collect())
    }
}

impl DbVNManager {
    pub fn get_all_that_can_execute_read_query(
        &self,
        tableops: &TableOps,
        txvn: &TxVN,
    ) -> Vec<(SocketAddr, Vec<DbTableVN>)> {
        self.0
            .iter()
            .filter(|(_, dbvn)| dbvn.can_execute_query(tableops, txvn))
            .map(|(addr, dbvn)| (addr.clone(), dbvn.get_from_tableops(tableops)))
            .collect()
    }

    pub fn release_version(&mut self, dbproxy_addr: SocketAddr, txvn: &TxVN) {
        if !self.0.contains_key(&dbproxy_addr) {
            warn!(
                "DbVNManager does not have a DbVN for {} yet, is this a newly added dbproxy?",
                dbproxy_addr
            );
        }
        self.0.entry(dbproxy_addr).or_default().release_version(txvn);
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
