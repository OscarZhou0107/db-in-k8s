use crate::core::*;
use crate::util::tcp::TcpStreamConnectionManager;
use bb8::Pool;
use futures::prelude::*;
use itertools::Itertools;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::net::SocketAddr;
use tracing::warn;

#[derive(Debug)]
pub struct ConnectionState {
    client_addr: SocketAddr,
    cur_txid: usize,
    cur_txvn: Option<TxVN>,
}

impl ConnectionState {
    pub fn new(client_addr: SocketAddr) -> Self {
        Self {
            client_addr,
            cur_txid: 0,
            cur_txvn: None,
        }
    }
}

impl ConnectionState {
    pub fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    pub fn current_txid(&self) -> usize {
        self.cur_txid
    }

    pub fn current_txvn(&self) -> &Option<TxVN> {
        &self.cur_txvn
    }

    pub fn replace_txvn(&mut self, new_txvn: Option<TxVN>) -> Option<TxVN> {
        let old_txvn = self.cur_txvn.take();
        self.cur_txvn = new_txvn;
        old_txvn
    }

    pub fn transaction_finished(&mut self) {
        self.cur_txid += 1;
    }
}

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
        assert_eq!(
            tableops.access_pattern(),
            AccessPattern::ReadOnly,
            "Expecting ReadOnly access pattern for the query"
        );

        let txtablevns = txvn
            .get_from_tableops(tableops)
            .expect("Mismatching between TableOps and TxVN");

        self.0
            .iter()
            .filter(|(_, dbvn)| dbvn.can_execute_query(&txtablevns))
            .map(|(addr, dbvn)| (addr.clone(), dbvn.get_from_tableops(tableops)))
            .sorted_by_key(|(addr, _)| *addr)
            .collect()
    }

    pub fn release_version(&mut self, dbproxy_addr: &SocketAddr, release_request: DbVNReleaseRequest) {
        if !self.0.contains_key(dbproxy_addr) {
            warn!(
                "DbVNManager does not have a DbVN for {} yet, is this a newly added dbproxy?",
                dbproxy_addr
            );
        }
        self.0
            .entry(dbproxy_addr.clone())
            .or_default()
            .release_version(release_request);
    }

    pub fn inner(&self) -> &HashMap<SocketAddr, DbVN> {
        &self.0
    }
}

#[derive(Clone)]
pub struct DbproxyManager(HashMap<SocketAddr, Pool<TcpStreamConnectionManager>>);

impl DbproxyManager {
    /// Converts an `Iterator<Item = dbproxy_port: SocketAddr>` with `max_conn: u32` 'into `DbproxyManager`
    pub async fn from_iter<I>(iter: I, max_conn: u32) -> Self
    where
        I: IntoIterator<Item = SocketAddr>,
    {
        Self(
            stream::iter(iter)
                .then(|dbproxy_port| async move {
                    (
                        dbproxy_port.clone(),
                        Pool::builder()
                            .max_size(max_conn)
                            .build(TcpStreamConnectionManager::new(dbproxy_port).await)
                            .await
                            .unwrap(),
                    )
                })
                .collect()
                .await,
        )
    }

    pub fn inner(&self) -> &HashMap<SocketAddr, Pool<TcpStreamConnectionManager>> {
        &self.0
    }

    pub fn get(&self, dbproxy_addr: &SocketAddr) -> Pool<TcpStreamConnectionManager> {
        self.0
            .get(dbproxy_addr)
            .expect(&format!("{} is not in the DbproxyManager", dbproxy_addr))
            .clone()
    }

    pub fn to_vec(&self) -> Vec<(SocketAddr, Pool<TcpStreamConnectionManager>)> {
        self.0.iter().map(|(addr, pool)| (addr.clone(), pool.clone())).collect()
    }
}

/// Unit test for `ConnectionState`
#[cfg(test)]
mod tests_connection_state {
    use super::*;

    #[test]
    fn test_replace_txvn() {
        let mut conn_state = ConnectionState::new("127.0.0.1:6666".parse().unwrap());
        assert_eq!(*conn_state.current_txvn(), None);
        assert_eq!(conn_state.replace_txvn(Some(TxVN::default())), None);
        assert_eq!(*conn_state.current_txvn(), Some(TxVN::default()));
        assert_eq!(conn_state.replace_txvn(None), Some(TxVN::default()));
        assert_eq!(*conn_state.current_txvn(), None);
    }

    #[test]
    fn test_transaction_finished() {
        let mut conn_state = ConnectionState::new("127.0.0.1:6666".parse().unwrap());
        assert_eq!(0, conn_state.current_txid());
        conn_state.transaction_finished();
        assert_eq!(1, conn_state.current_txid());
    }
}

/// Unit test for `DbVNManager`
#[cfg(test)]
mod tests_dbvnmanager {
    use super::*;

    #[test]
    fn test_from_iter() {
        let dbvnmanager = DbVNManager::from_iter(vec![
            "127.0.0.1:10000".parse().unwrap(),
            "127.0.0.1:10001".parse().unwrap(),
            "127.0.0.1:10002".parse().unwrap(),
        ]);

        assert!(dbvnmanager.inner().contains_key(&"127.0.0.1:10000".parse().unwrap()));
        assert!(dbvnmanager.inner().contains_key(&"127.0.0.1:10001".parse().unwrap()));
        assert!(dbvnmanager.inner().contains_key(&"127.0.0.1:10002".parse().unwrap()));
        assert!(!dbvnmanager.inner().contains_key(&"127.0.0.1:10003".parse().unwrap()));
    }

    #[test]
    fn test_get_all_that_can_execute_read_query() {
        let dbvnmanager = DbVNManager::from_iter(vec![
            "127.0.0.1:10000".parse().unwrap(),
            "127.0.0.1:10001".parse().unwrap(),
        ]);

        assert_eq!(
            dbvnmanager.get_all_that_can_execute_read_query(
                &TableOps::from_iter(vec![
                    TableOp::new("t0", RWOperation::R),
                    TableOp::new("t1", RWOperation::R)
                ]),
                &TxVN {
                    tx: None,
                    txtablevns: vec![
                        TxTableVN::new("t0", 0, RWOperation::R),
                        TxTableVN::new("t1", 0, RWOperation::R),
                    ],
                }
            ),
            vec![
                (
                    "127.0.0.1:10000".parse().unwrap(),
                    vec![DbTableVN::new("t0", 0), DbTableVN::new("t1", 0)]
                ),
                (
                    "127.0.0.1:10001".parse().unwrap(),
                    vec![DbTableVN::new("t0", 0), DbTableVN::new("t1", 0)]
                )
            ]
        );

        assert_eq!(
            dbvnmanager.get_all_that_can_execute_read_query(
                &TableOps::from_iter(vec![
                    TableOp::new("t0", RWOperation::R),
                    TableOp::new("t1", RWOperation::R)
                ]),
                &TxVN {
                    tx: None,
                    txtablevns: vec![
                        TxTableVN::new("t0", 0, RWOperation::R),
                        TxTableVN::new("t1", 1, RWOperation::R),
                    ],
                }
            ),
            vec![]
        );

        assert_eq!(
            dbvnmanager.get_all_that_can_execute_read_query(
                &TableOps::from_iter(vec![TableOp::new("t0", RWOperation::R)]),
                &TxVN {
                    tx: None,
                    txtablevns: vec![
                        TxTableVN::new("t0", 0, RWOperation::R),
                        TxTableVN::new("t1", 1, RWOperation::R),
                    ],
                }
            ),
            vec![
                ("127.0.0.1:10000".parse().unwrap(), vec![DbTableVN::new("t0", 0)]),
                ("127.0.0.1:10001".parse().unwrap(), vec![DbTableVN::new("t0", 0)])
            ]
        );

        assert_eq!(
            dbvnmanager.get_all_that_can_execute_read_query(
                &TableOps::from_iter(vec![TableOp::new("t1", RWOperation::R)]),
                &TxVN {
                    tx: None,
                    txtablevns: vec![
                        TxTableVN::new("t0", 0, RWOperation::R),
                        TxTableVN::new("t1", 1, RWOperation::R),
                    ],
                }
            ),
            vec![]
        );
    }

    #[test]
    #[should_panic]
    fn test_get_all_that_can_execute_read_query_panic() {
        let dbvnmanager = DbVNManager::from_iter(vec![
            "127.0.0.1:10000".parse().unwrap(),
            "127.0.0.1:10001".parse().unwrap(),
        ]);

        dbvnmanager.get_all_that_can_execute_read_query(
            &TableOps::from_iter(vec![TableOp::new("t0", RWOperation::W)]),
            &TxVN {
                tx: None,
                txtablevns: vec![
                    TxTableVN::new("t0", 0, RWOperation::W),
                    TxTableVN::new("t1", 0, RWOperation::W),
                ],
            },
        );
    }

    #[test]
    fn test_release_version() {
        let mut dbvnmanager = DbVNManager::from_iter(vec![
            "127.0.0.1:10000".parse().unwrap(),
            "127.0.0.1:10001".parse().unwrap(),
        ]);

        let txvn0 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 0, RWOperation::R),
            ],
        };
        assert_eq!(
            dbvnmanager.get_all_that_can_execute_read_query(
                &TableOps::from_iter(vec![
                    TableOp::new("t0", RWOperation::R),
                    TableOp::new("t1", RWOperation::R)
                ]),
                &txvn0
            ),
            vec![
                (
                    "127.0.0.1:10000".parse().unwrap(),
                    vec![DbTableVN::new("t0", 0), DbTableVN::new("t1", 0)]
                ),
                (
                    "127.0.0.1:10001".parse().unwrap(),
                    vec![DbTableVN::new("t0", 0), DbTableVN::new("t1", 0)]
                )
            ]
        );

        let txvn1 = TxVN {
            tx: None,
            txtablevns: vec![
                TxTableVN::new("t0", 0, RWOperation::R),
                TxTableVN::new("t1", 1, RWOperation::R),
            ],
        };
        assert_eq!(
            dbvnmanager.get_all_that_can_execute_read_query(
                &TableOps::from_iter(vec![
                    TableOp::new("t0", RWOperation::R),
                    TableOp::new("t1", RWOperation::R)
                ]),
                &txvn1
            ),
            vec![]
        );

        dbvnmanager.release_version(
            &"127.0.0.1:10000".parse().unwrap(),
            txvn0.clone().into_dbvn_release_request(),
        );
        assert_eq!(
            dbvnmanager.get_all_that_can_execute_read_query(
                &TableOps::from_iter(vec![
                    TableOp::new("t0", RWOperation::R),
                    TableOp::new("t1", RWOperation::R)
                ]),
                &txvn1
            ),
            vec![(
                "127.0.0.1:10000".parse().unwrap(),
                vec![DbTableVN::new("t0", 1), DbTableVN::new("t1", 1)]
            )]
        );

        dbvnmanager.release_version(
            &"127.0.0.1:10001".parse().unwrap(),
            txvn0.clone().into_dbvn_release_request(),
        );
        assert_eq!(
            dbvnmanager.get_all_that_can_execute_read_query(
                &TableOps::from_iter(vec![
                    TableOp::new("t0", RWOperation::R),
                    TableOp::new("t1", RWOperation::R)
                ]),
                &txvn1
            ),
            vec![
                (
                    "127.0.0.1:10000".parse().unwrap(),
                    vec![DbTableVN::new("t0", 1), DbTableVN::new("t1", 1)]
                ),
                (
                    "127.0.0.1:10001".parse().unwrap(),
                    vec![DbTableVN::new("t0", 1), DbTableVN::new("t1", 1)]
                )
            ]
        );
    }
}
