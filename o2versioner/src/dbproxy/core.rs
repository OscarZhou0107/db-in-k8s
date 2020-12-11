use crate::comm::MsqlResponse;
use crate::core::*;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use csv::Writer;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio_postgres::SimpleQueryMessage;
use tracing::debug;

#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub arriving_timestamp: DateTime<Utc>,
    pub identifier: RequestMeta,
    pub operation_type: Task,
    pub msql: Msql,
    pub versions: Option<TxVN>,
}

impl QueueMessage {
    pub fn new(identifier: RequestMeta, msql: Msql, versions: Option<TxVN>) -> Self {
        let operation_type = match &msql {
            Msql::BeginTx(_) => Task::BEGIN,
            Msql::Query(op) => match op.tableops().access_pattern() {
                AccessPattern::ReadOnly => {
                    assert!(!op.has_early_release());
                    Task::READ
                }
                AccessPattern::WriteOnly => Task::WRITE,
                _ => panic!("Illegal access pattern"),
            },
            Msql::EndTx(op) => match op.mode() {
                MsqlEndTxMode::Commit => Task::COMMIT,
                MsqlEndTxMode::Rollback => Task::ABORT,
            },
        };

        Self {
            arriving_timestamp: Utc::now(),
            identifier,
            operation_type,
            msql,
            versions,
        }
    }

    pub fn into_sqlresponse(self, raw: Result<Vec<SimpleQueryMessage>, tokio_postgres::error::Error>) -> QueryResult {
        let result;
        let succeed;
        let writer = PostgreToCsvWriter::new(self.operation_type.clone());
        match raw {
            Ok(message) => {
                result = writer.to_csv(message);
                succeed = true;
            }
            Err(err) => {
                result = err.to_string();
                succeed = false;
            }
        }

        let result_type = match self.operation_type {
            Task::READ | Task::WRITE => QueryResultType::QUERY,
            Task::COMMIT | Task::ABORT => QueryResultType::END,
            _ => {
                panic!("Illegal operation");
            }
        };

        QueryResult {
            identifier: self.identifier,
            result,
            result_type,
            succeed,
            contained_newer_versions: self.versions.unwrap(),
            contained_early_release_version: self.msql.try_get_query().ok().and_then(|q| {
                if q.has_early_release() {
                    Some(q.early_release_tables().clone())
                } else {
                    None
                }
            }),
        }
    }
}

pub struct PendingQueue {
    pub queue: Vec<QueueMessage>,
    pub notify: Arc<Notify>,
}

impl PendingQueue {
    pub fn new() -> Self {
        Self {
            queue: Vec::new(),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn push(&mut self, op: QueueMessage) {
        debug!("PendingQueue pushed {:?} and notify all tasks waiting", op);
        self.queue.push(op);
        self.notify.notify_one();
    }

    pub fn get_notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    pub async fn get_all_version_ready_task(&mut self, version: Arc<Mutex<DbVersion>>) -> Vec<QueueMessage> {
        let mut ready_queue = Vec::new();
        let mut unready_queue = Vec::new();
        let mut tx_set = HashSet::new();
        for operation in self.queue.drain(..) {
            let uuid = operation.versions.as_ref().unwrap().uuid().clone();
            if tx_set.insert(uuid) {
                let violate_version = if let Some(txvn) = &operation.versions {
                    match &operation.msql {
                        Msql::Query(query) => {
                            version
                                .lock()
                                .map(|version| {
                                    let tbops = query.tableops();
                                    let ertables = query.early_release_tables();
                                    version.violate_version(&txvn.get_from_tableops(tbops).unwrap())
                                        || version.violate_version(&txvn.get_from_ertables(ertables).unwrap())
                                })
                                .await
                        }
                        Msql::EndTx(_) => version.lock().await.violate_version(txvn.txtablevns()),
                        _ => false,
                    }
                } else {
                    // If no txvn, then not going to be blocked for version
                    false
                };

                if violate_version {
                    unready_queue.push(operation);
                } else {
                    ready_queue.push(operation);
                }
            } else {
                unready_queue.push(operation);
            }
        }

        self.queue = unready_queue;

        ready_queue
    }
}

pub struct DbVersion {
    pub db_version: DbVN,
    notify: Arc<Notify>,
}

impl DbVersion {
    pub fn new(db_versions: DbVN) -> Self {
        Self {
            db_version: db_versions,
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn release_on_transaction(&mut self, transaction_version: TxVN) {
        self.release_on_request(transaction_version.into_dbvn_release_request());
    }

    pub fn release_on_request(&mut self, release_request: DbVNReleaseRequest) {
        debug!("Versions before release: {:?}", self.db_version);
        self.db_version.release_version(release_request);
        debug!("Versions after release: {:?}", self.db_version);
        self.notify.notify_one();
    }

    pub fn violate_version(&self, txtablevns: &[TxTableVN]) -> bool {
        !self.db_version.can_execute_query(txtablevns)
    }

    pub fn get_notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }
}

#[async_trait]
trait Repository {
    async fn start_transaction(&mut self);
    async fn execute_read(&mut self) -> QueryResult;
    async fn execute_write(&mut self) -> QueryResult;
    async fn commit(&mut self) -> QueryResult;
    async fn abort(&mut self) -> QueryResult;
}

// pub struct PostgresSqlRepository {
//     conn : PooledConnection<PostgresConnectionManager<NoTls>>,
// }

// impl PostgresSqlRepository {
//     pub async fn new(pool : PostgresSqlConnPool) -> Self{
//         let conn = pool.get_conn().await;
//         PostgresSqlRepository {conn : conn}
//     }
// }

// #[async_trait]
// impl Repository for PostgresSqlRepository {
//     async fn start_transaction(&mut self) {
//     }

//     async fn execute_read(&mut self) -> QueryResult {
//         todo!()
//     }

//     async fn execute_write(&mut self) -> QueryResult {
//         todo!()
//     }

//     async fn commit(&mut self) -> QueryResult {
//         todo!()
//     }

//     async fn abort(&mut self) -> QueryResult {
//         todo!()
//     }
// }

pub struct PostgreToCsvWriter {
    mode: Task,
    wrt: Writer<Vec<u8>>,
}

#[derive(Serialize)]
pub struct Row {
    label: String,
    value: String,
}

impl PostgreToCsvWriter {
    pub fn new(mode: Task) -> Self {
        let wrt = Writer::from_writer(vec![]);
        Self { mode: mode, wrt: wrt }
    }

    pub fn to_csv(self, message: Vec<tokio_postgres::SimpleQueryMessage>) -> String {
        match self.mode {
            Task::BEGIN => return "".to_string(),
            Task::READ => return self.convert_result_to_csv_string(message),
            Task::WRITE => return self.generate_csv_string_with_header(message, "Affected rows".to_string()),
            Task::COMMIT => return self.generate_csv_string_with_header(message, "Status".to_string()),
            Task::ABORT => return self.generate_csv_string_with_header(message, "Status".to_string()),
        }
    }

    pub fn convert_result_to_csv_string(mut self, message: Vec<tokio_postgres::SimpleQueryMessage>) -> String {
        message.iter().for_each(|q_message| match q_message {
            tokio_postgres::SimpleQueryMessage::Row(query_row) => {
                let len = query_row.len();
                let mut row: Vec<String> = Vec::new();
                row.reserve(len);

                for index in 0..len {
                    row.push(format!("\"{}\"", query_row.get(index).unwrap()));
                }

                self.wrt.write_record(&row).unwrap();
            }
            _ => {}
        });

        String::from_utf8(self.wrt.into_inner().unwrap()).unwrap()
    }

    pub fn generate_csv_string_with_header(
        mut self,
        message: Vec<tokio_postgres::SimpleQueryMessage>,
        header: String,
    ) -> String {
        self.wrt.write_record(vec![header]).unwrap();

        message.iter().for_each(|q_message| match q_message {
            tokio_postgres::SimpleQueryMessage::CommandComplete(status) => {
                self.wrt.write_record(vec![status.clone().to_string()]).unwrap();
            }
            _ => {}
        });

        String::from_utf8(self.wrt.into_inner().unwrap()).unwrap()
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub enum QueryResultType {
    BEGIN,
    QUERY,
    END,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct QueryResult {
    pub identifier: RequestMeta,
    pub result: String,
    pub succeed: bool,
    pub result_type: QueryResultType,
    pub contained_newer_versions: TxVN,
    pub contained_early_release_version: Option<EarlyReleaseTables>,
}

impl QueryResult {
    pub fn into_msql_response(self) -> MsqlResponse {
        let message = match self.result_type {
            QueryResultType::BEGIN => {
                if self.succeed {
                    MsqlResponse::begintx_ok()
                } else {
                    MsqlResponse::begintx_err(self.result)
                }
            }
            QueryResultType::QUERY => {
                if self.succeed {
                    MsqlResponse::query_ok(self.result)
                } else {
                    MsqlResponse::query_err(self.result)
                }
            }
            QueryResultType::END => {
                if self.succeed {
                    MsqlResponse::endtx_ok(self.result)
                } else {
                    MsqlResponse::endtx_err(self.result)
                }
            }
        };

        message
    }

    pub fn acquire_early_release(&mut self) -> Result<DbVNReleaseRequest, &'static str> {
        if let Some(early_release) = self.contained_early_release_version.take() {
            return self.contained_newer_versions.early_release_request(early_release);
        }
        Err("No available early release")
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum Task {
    BEGIN,
    READ,
    WRITE,
    ABORT,
    COMMIT,
}

//================================Test================================//

// #[cfg(test)]
// mod tests_dbproxy_core {
//     use super::DbVersion;
//     use super::Task;
//     use super::TxTableVN;
//     use crate::core::operation::Operation as RWOperation;
//     use bb8_postgres::{bb8::Pool, PostgresConnectionManager};
//     use mysql_async::prelude::Queryable;
//     use tokio_postgres::NoTls;
//     use std::collections::HashMap;

//     #[test]
//     fn voilate_dbversion_should_return_true() {
//         //Prepare
//         let mut table_versions = HashMap::new();
//         table_versions.insert("table1".to_string(), 0);
//         table_versions.insert("table2".to_string(), 0);
//         let db_version = DbVersion::new(table_versions);
//         let versions = vec![
//             TxTableVN {
//                 table: "table1".to_string(),
//                 vn: 0,
//                 op: RWOperation::R,
//             },
//             TxTableVN {
//                 table: "table2".to_string(),
//                 vn: 1,
//                 op: RWOperation::R,
//             },
//         ];
//         let operation = Operation {
//             txtablevns: versions,
//             transaction_id: "t1".to_string(),
//             task: Task::READ,
//         };
//         //Action
//         //Assert
//         assert!(db_version.violate_version(operation));
//     }

//     #[test]
//     fn obey_dbversion_should_return_false() {
//         //Prepare
//         let mut table_versions = HashMap::new();
//         table_versions.insert("table1".to_string(), 0);
//         table_versions.insert("table2".to_string(), 0);
//         let db_version = DbVersion::new(table_versions);
//         let versions = vec![
//             TxTableVN {
//                 table: "table1".to_string(),
//                 vn: 0,
//                 op: RWOperation::R,
//             },
//             TxTableVN {
//                 table: "table2".to_string(),
//                 vn: 0,
//                 op: RWOperation::R,
//             },
//         ];
//         let operation = Operation {
//             txtablevns: versions,
//             transaction_id: "t1".to_string(),
//             task: Task::READ,
//         };
//         //Action
//         //Assert
//         assert!(!db_version.violate_version(operation));
//     }

#[cfg(test)]
mod tests {
    use super::*;
    use bb8::Pool;
    use bb8_postgres::PostgresConnectionManager;
    use tokio_postgres::NoTls;

    #[test]
    #[ignore]
    fn postgres_write_test() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async move {
            let mut config = tokio_postgres::Config::new();
            config.user("postgres");
            config.password("Rayh8768");
            config.host("localhost");
            config.port(5432);
            config.dbname("Test");

            let size: u32 = 50;
            let manager = PostgresConnectionManager::new(config, NoTls);
            let pool = Pool::builder().max_size(size).build(manager).await.unwrap();

            let conn = pool.get().await.unwrap();
            conn.simple_query("START TRANSACTION;").await.unwrap();
            let result = conn
                .simple_query(
                    "INSERT INTO tbltest (name, age, designation, salary) VALUES ('haha', 100, 'Manager', 99999)",
                )
                .await
                .unwrap();
            conn.simple_query("COMMIT;").await.unwrap();

            result.iter().for_each(|q_message| match q_message {
                tokio_postgres::SimpleQueryMessage::Row(query_row) => {
                    let len = query_row.len();
                    for index in 0..len {
                        println!("value is : {}", query_row.get(index).unwrap().to_string());
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(complete_status) => {
                    println!("Command result is : {}", complete_status);
                }
                _ => {}
            });

            let writer = PostgreToCsvWriter::new(Task::WRITE);
            let csv = writer.to_csv(result);

            println!("Converted string is: {}", csv);
        });
    }

    #[test]
    fn postgres_read_test() {
        let rt = tokio::runtime::Runtime::new().unwrap();

        rt.block_on(async move {
            let mut config = tokio_postgres::Config::new();
            config.user("postgres");
            config.password("Abc@123");
            config.host("34.70.165.61");
            config.port(5432);
            config.dbname("tpcw");

            let size: u32 = 50;
            let manager = PostgresConnectionManager::new(config, NoTls);
            let pool = Pool::builder().max_size(size).build(manager).await.unwrap();

            let conn = pool.get().await.unwrap();
            let result = conn.simple_query("SELECT * FROM address LIMIT 10;").await.unwrap();

            result.iter().for_each(|q_message| match q_message {
                tokio_postgres::SimpleQueryMessage::Row(query_row) => {
                    let len = query_row.len();
                    for index in 0..len {
                        println!("value is : {}", query_row.get(index).unwrap().to_string());
                    }
                }
                tokio_postgres::SimpleQueryMessage::CommandComplete(complete_status) => {
                    println!("Command result is : {}", complete_status);
                }
                _ => {}
            });

            let writer = PostgreToCsvWriter::new(Task::READ);
            let csv = writer.to_csv(result);

            println!("Converted string is: {}", csv);
        });
    }

    #[tokio::test]
    #[ignore]
    async fn pending_queue_task_order_test() {
        let dbversion = Arc::new(Mutex::new(DbVersion::new(Default::default())));
        let mut queue = PendingQueue::new();

        let message4 = QueueMessage::new(
            RequestMeta {
                client_addr: "127.0.0.4:8080".parse().unwrap(),
                cur_txid: 0,
                request_id: 0,
            },
            Msql::Query(
                MsqlQuery::new(
                    "select * from tbltest",
                    TableOps::from("READ table0 table1"),
                    EarlyReleaseTables::default(),
                )
                .unwrap(),
            ),
            None,
        );

        let message3 = QueueMessage::new(
            RequestMeta {
                client_addr: "127.0.0.3:8080".parse().unwrap(),
                cur_txid: 0,
                request_id: 0,
            },
            Msql::Query(
                MsqlQuery::new(
                    "select * from tbltest",
                    TableOps::from("READ table0 table1"),
                    EarlyReleaseTables::from(""),
                )
                .unwrap(),
            ),
            Some(TxVN::new()),
        );

        let message2 = QueueMessage::new(
            RequestMeta {
                client_addr: "127.0.0.2:8080".parse().unwrap(),
                cur_txid: 0,
                request_id: 0,
            },
            Msql::Query(
                MsqlQuery::new(
                    "select * from tbltest",
                    TableOps::from("READ table0 table1"),
                    EarlyReleaseTables::default(),
                )
                .unwrap(),
            ),
            Some(TxVN::new()),
        );

        let message1 = QueueMessage::new(
            RequestMeta {
                client_addr: "127.0.0.1:8080".parse().unwrap(),
                cur_txid: 0,
                request_id: 0,
            },
            Msql::Query(
                MsqlQuery::new(
                    "select * from tbltest",
                    TableOps::from("READ table0 table1"),
                    EarlyReleaseTables::default(),
                )
                .unwrap(),
            ),
            None,
        );

        queue.push(message1);
        queue.push(message2);
        queue.push(message3);
        queue.push(message4);

        let ready_tasks = queue.get_all_version_ready_task(dbversion).await;

        ready_tasks.iter().for_each(|task| {
            println!("{}", task.identifier);
        });

        queue.queue.iter().for_each(|task| {
            println!("{}", task.identifier);
        });
    }

    #[tokio::test]
    #[ignore]
    async fn pending_queue_task_order_test_2() {
        let dbversion = Arc::new(Mutex::new(DbVersion::new(Default::default())));
        let mut queue = PendingQueue::new();

        let message4 = QueueMessage::new(
            RequestMeta {
                client_addr: "127.0.0.4:8080".parse().unwrap(),
                cur_txid: 0,
                request_id: 0,
            },
            Msql::Query(
                MsqlQuery::new(
                    "select * from tbltest",
                    TableOps::from("READ table0 table1"),
                    EarlyReleaseTables::default(),
                )
                .unwrap(),
            ),
            None,
        );

        let message3 = QueueMessage::new(
            RequestMeta {
                client_addr: "127.0.0.3:8080".parse().unwrap(),
                cur_txid: 0,
                request_id: 0,
            },
            Msql::Query(
                MsqlQuery::new(
                    "select * from tbltest",
                    TableOps::from("READ table0 table1"),
                    EarlyReleaseTables::default(),
                )
                .unwrap(),
            ),
            None,
        );

        let message2 = QueueMessage::new(
            RequestMeta {
                client_addr: "127.0.0.2:8080".parse().unwrap(),
                cur_txid: 0,
                request_id: 0,
            },
            Msql::Query(
                MsqlQuery::new(
                    "select * from tbltest",
                    TableOps::from("READ table0 table1"),
                    EarlyReleaseTables::default(),
                )
                .unwrap(),
            ),
            None,
        );

        let message1 = QueueMessage::new(
            RequestMeta {
                client_addr: "127.0.0.1:8080".parse().unwrap(),
                cur_txid: 0,
                request_id: 0,
            },
            Msql::Query(
                MsqlQuery::new(
                    "select * from tbltest",
                    TableOps::from("READ table0 table1"),
                    EarlyReleaseTables::default(),
                )
                .unwrap(),
            ),
            None,
        );

        queue.push(message1);
        queue.push(message2);
        queue.push(message3);
        queue.push(message4);

        let ready_tasks = queue.get_all_version_ready_task(dbversion).await;

        ready_tasks.iter().for_each(|task| {
            println!("{}", task.identifier);
        });

        queue.queue.iter().for_each(|task| {
            println!("{}", task.identifier);
        });

        println!("Number of tasks is: {}", ready_tasks.len());
        assert!(ready_tasks.len() == 4);
    }
}
