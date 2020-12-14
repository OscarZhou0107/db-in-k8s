use super::logging::*;
use super::transceiver::TransceiverAddr;
use crate::core::*;
use crate::util::common::{create_zip_csv_writer, prepare_logging_dir};
use crate::util::config::*;
use itertools::Itertools;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{info, warn};

#[derive(Debug, Clone)]
/// A collection of shared variables across
/// the entire scheduler
pub struct State {
    dbvn_manager: Arc<RwLock<DbVNManager>>,
    client_records: ClientRecords,
    conf: Arc<Config>,
}

impl State {
    pub fn new(dbvn_manager: DbVNManager, conf: Config) -> Self {
        Self {
            dbvn_manager: Arc::new(RwLock::new(dbvn_manager)),
            client_records: ClientRecords::new(conf.scheduler.detailed_logging.is_some()),
            conf: Arc::new(conf),
        }
    }

    pub fn share_dbvn_manager(&self) -> Arc<RwLock<DbVNManager>> {
        self.dbvn_manager.clone()
    }

    pub async fn share_client_record(&self, client: SocketAddr) -> Arc<RwLock<ClientRecord>> {
        self.client_records.share_client_record(client).await
    }

    /// Dump the performance logging, returns the path of logging dumps if dumped
    pub async fn dump_perf_log(&self) -> Option<String> {
        if let Some(perf_log_path) = self.conf.scheduler.performance_logging.as_ref() {
            // Prepare the logging directory
            let cur_log_dir = prepare_logging_dir(perf_log_path).await;

            // Performance logging
            let mut perf_csv_path_builder = PathBuf::from(&cur_log_dir);
            perf_csv_path_builder.push("perf.csv.gz");
            let perf_csv_path = perf_csv_path_builder.as_path();
            let mut wrt = create_zip_csv_writer(perf_csv_path).unwrap();
            self.client_records
                .collect_flattened_performance()
                .await
                .into_iter()
                .for_each(|r| wrt.serialize(r).unwrap());
            info!("Dumped performance logging to {}", perf_csv_path.display());

            // Dbvn logging
            let mut dbproxy_stats_path_builder = PathBuf::from(&cur_log_dir);
            dbproxy_stats_path_builder.push("dbproxy_stats.csv.gz");
            let dbproxy_stats_csv_path = dbproxy_stats_path_builder.as_path();
            let mut wrt = create_zip_csv_writer(dbproxy_stats_csv_path).unwrap();
            wrt.write_record(&["dbproxy_addr", "dbproxy_vn_sum"]).unwrap();
            self.dbvn_manager
                .read()
                .await
                .inner()
                .iter()
                .map(|(dbproxy_addr, vndb)| {
                    info!("{} {:?}", dbproxy_addr, vndb);
                    (dbproxy_addr, vndb)
                })
                .map(|(dbproxy_addr, vndb)| (dbproxy_addr.clone(), vndb.get_version_sum()))
                .for_each(|d| wrt.serialize(d).unwrap());
            info!("Dumped dbproxy stats to {}", dbproxy_stats_csv_path.display());
            Some(dbproxy_stats_csv_path.to_string_lossy().to_string())
        } else {
            info!("Due to the conf setting, performance loggings are not dumped");
            None
        }
    }
}

#[derive(Debug)]
/// Data relating to a specific client connection.
/// This state should only have a single copy, and
/// should not need to be accessed concurrently
pub struct ConnectionState {
    client_meta: ClientMeta,
    cur_txvn: Option<TxVN>,
    client_record: Arc<RwLock<ClientRecord>>,
}

impl ConnectionState {
    pub fn new(client_addr: SocketAddr, client_record: Arc<RwLock<ClientRecord>>) -> Self {
        Self {
            client_meta: ClientMeta::new(client_addr),
            cur_txvn: None,
            client_record,
        }
    }

    pub fn client_meta(&self) -> &ClientMeta {
        &self.client_meta
    }

    pub fn client_meta_as_mut(&mut self) -> &mut ClientMeta {
        &mut self.client_meta
    }

    pub fn current_txvn(&self) -> &Option<TxVN> {
        &self.cur_txvn
    }

    pub fn replace_txvn(&mut self, new_txvn: Option<TxVN>) -> Option<TxVN> {
        let old_txvn = self.cur_txvn.take();
        self.cur_txvn = new_txvn;
        old_txvn
    }

    pub async fn current_request_id(&self) -> usize {
        self.client_record.read().await.len()
    }

    pub async fn push_request_record(&self, request_record: RequestRecord) {
        self.client_record.write().await.push(request_record)
    }
}

/// `Dbproxy_addr` -> `DvVN`
#[derive(Debug)]
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
    // pub fn get_all_addr(&self) -> Vec<SocketAddr> {
    //     self.0.iter().map(|(addr, _)| addr.clone()).collect()
    // }

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

    /// Uses the sum of VNs of interest to determine the most updated
    pub fn get_most_updated_version_for_read_query(&self, tableops: &TableOps) -> (SocketAddr, Vec<DbTableVN>) {
        assert_eq!(
            tableops.access_pattern(),
            AccessPattern::ReadOnly,
            "Expecting ReadOnly access pattern for the query"
        );

        self.0
            .iter()
            .max_by_key(|(_, dbvn)| {
                dbvn.get_from_tableops(tableops)
                    .iter()
                    .map(|dbtablevn| dbtablevn.vn)
                    .sum::<VN>()
            })
            .map(|(addr, dbvn)| (addr.clone(), dbvn.get_from_tableops(tableops)))
            .unwrap()
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
pub struct DbproxyManager(HashMap<SocketAddr, TransceiverAddr>);

impl FromIterator<(SocketAddr, TransceiverAddr)> for DbproxyManager {
    fn from_iter<I: IntoIterator<Item = (SocketAddr, TransceiverAddr)>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl DbproxyManager {
    pub fn inner(&self) -> &HashMap<SocketAddr, TransceiverAddr> {
        &self.0
    }

    pub fn get(&self, dbproxy_addr: &SocketAddr) -> TransceiverAddr {
        self.0
            .get(dbproxy_addr)
            .expect(&format!("{} is not in the DbproxyManager", dbproxy_addr))
            .clone()
    }

    pub fn to_vec(&self) -> Vec<(SocketAddr, TransceiverAddr)> {
        self.0
            .iter()
            .map(|(addr, transceiver_addr)| (addr.clone(), transceiver_addr.clone()))
            .collect()
    }
}

/// Unit test for `ConnectionState`
#[cfg(test)]
mod tests_connection_state {
    use super::*;

    #[test]
    fn test_replace_txvn() {
        let client_addr: SocketAddr = "127.0.0.1:6666".parse().unwrap();
        let mut conn_state = ConnectionState::new(
            client_addr.clone(),
            Arc::new(RwLock::new(ClientRecord::new(client_addr, false))),
        );
        assert_eq!(*conn_state.current_txvn(), None);

        assert_eq!(conn_state.replace_txvn(Some(TxVN::new().erase_uuid())), None);
        assert_eq!(
            *conn_state.current_txvn(),
            Some(TxVN::new()).map(|txvn| txvn.erase_uuid())
        );
        assert_eq!(
            conn_state.replace_txvn(None),
            Some(TxVN::new()).map(|txvn| txvn.erase_uuid())
        );
        assert_eq!(*conn_state.current_txvn(), None);
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
                &TxVN::new().set_txtablevns(vec![
                    TxTableVN::new("t0", 0, RWOperation::R),
                    TxTableVN::new("t1", 0, RWOperation::R),
                ])
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
                &TxVN::new().set_txtablevns(vec![
                    TxTableVN::new("t0", 0, RWOperation::R),
                    TxTableVN::new("t1", 1, RWOperation::R),
                ])
            ),
            vec![]
        );

        assert_eq!(
            dbvnmanager.get_all_that_can_execute_read_query(
                &TableOps::from_iter(vec![TableOp::new("t0", RWOperation::R)]),
                &TxVN::new().set_txtablevns(vec![
                    TxTableVN::new("t0", 0, RWOperation::R),
                    TxTableVN::new("t1", 1, RWOperation::R),
                ])
            ),
            vec![
                ("127.0.0.1:10000".parse().unwrap(), vec![DbTableVN::new("t0", 0)]),
                ("127.0.0.1:10001".parse().unwrap(), vec![DbTableVN::new("t0", 0)])
            ]
        );

        assert_eq!(
            dbvnmanager.get_all_that_can_execute_read_query(
                &TableOps::from_iter(vec![TableOp::new("t1", RWOperation::R)]),
                &TxVN::new().set_txtablevns(vec![
                    TxTableVN::new("t0", 0, RWOperation::R),
                    TxTableVN::new("t1", 1, RWOperation::R),
                ])
            ),
            vec![]
        );
    }

    #[test]
    fn test_get_most_updated_version_for_read_query() {
        let mut dbvnmanager = DbVNManager::from_iter(vec![
            "127.0.0.1:10000".parse().unwrap(),
            "127.0.0.1:10001".parse().unwrap(),
            "127.0.0.1:10002".parse().unwrap(),
        ]);

        dbvnmanager.release_version(
            &"127.0.0.1:10002".parse().unwrap(),
            TxVN::new()
                .set_txtablevns(vec![
                    TxTableVN::new("t0", 0, RWOperation::R),
                    TxTableVN::new("t1", 0, RWOperation::R),
                ])
                .into_dbvn_release_request(),
        );

        dbvnmanager.release_version(
            &"127.0.0.1:10001".parse().unwrap(),
            TxVN::new()
                .set_txtablevns(vec![
                    TxTableVN::new("t0", 0, RWOperation::W),
                    TxTableVN::new("t2", 0, RWOperation::W),
                ])
                .into_dbvn_release_request(),
        );

        assert_eq!(
            dbvnmanager.get_most_updated_version_for_read_query(&TableOps::from_iter(vec![
                TableOp::new("t0", RWOperation::R),
                TableOp::new("t1", RWOperation::R)
            ])),
            (
                "127.0.0.1:10002".parse().unwrap(),
                vec![DbTableVN::new("t0", 1), DbTableVN::new("t1", 1)]
            )
        );

        dbvnmanager.release_version(
            &"127.0.0.1:10001".parse().unwrap(),
            TxVN::new()
                .set_txtablevns(vec![
                    TxTableVN::new("t0", 0, RWOperation::W),
                    TxTableVN::new("t1", 0, RWOperation::W),
                ])
                .into_dbvn_release_request(),
        );

        assert_eq!(
            dbvnmanager.get_most_updated_version_for_read_query(&TableOps::from_iter(vec![
                TableOp::new("t0", RWOperation::R),
                TableOp::new("t1", RWOperation::R)
            ])),
            (
                "127.0.0.1:10001".parse().unwrap(),
                vec![DbTableVN::new("t0", 2), DbTableVN::new("t1", 1)]
            )
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
            &TxVN::new().set_txtablevns(vec![
                TxTableVN::new("t0", 0, RWOperation::W),
                TxTableVN::new("t1", 0, RWOperation::W),
            ]),
        );
    }

    #[test]
    fn test_release_version() {
        let mut dbvnmanager = DbVNManager::from_iter(vec![
            "127.0.0.1:10000".parse().unwrap(),
            "127.0.0.1:10001".parse().unwrap(),
        ]);

        let txvn0 = TxVN::new().set_txtablevns(vec![
            TxTableVN::new("t0", 0, RWOperation::R),
            TxTableVN::new("t1", 0, RWOperation::R),
        ]);
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

        let txvn1 = TxVN::new().set_txtablevns(vec![
            TxTableVN::new("t0", 0, RWOperation::R),
            TxTableVN::new("t1", 1, RWOperation::R),
        ]);
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
