use crate::comm::MsqlResponse;
use crate::core::DbVN;
use crate::core::{IntoMsqlFinalString, Msql, MsqlEndTxMode, RequestMeta, TxVN};
use async_trait::async_trait;
use bb8_postgres::bb8::{Pool, PooledConnection};
use bb8_postgres::PostgresConnectionManager;
use csv::Writer;
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

#[derive(Clone)]
pub struct PostgresSqlConnPool {
    pool: Pool<PostgresConnectionManager<NoTls>>,
}

impl PostgresSqlConnPool {
    pub async fn new(url: Config, max_conn: u32) -> Self {
        let manager = PostgresConnectionManager::new(url, NoTls);
        let pool = Pool::builder().max_size(max_conn).build(manager).await.unwrap();

        Self { pool: pool }
    }

    pub async fn get_conn(&self) -> PooledConnection<'_, PostgresConnectionManager<NoTls>> {
        self.pool.get().await.unwrap()
    }
}

#[derive(Clone)]
pub struct QueueMessage {
    pub identifier: RequestMeta,
    pub operation_type: Task,
    pub query: String,
    pub versions: Option<TxVN>,
}

impl QueueMessage {
    pub fn new(identifier: RequestMeta, request: Msql, versions: Option<TxVN>) -> Self {
        let operation_type;
        let mut query_string = String::new();

        match request {
            Msql::BeginTx(_) => {
                operation_type = Task::BEGIN;
            }

            Msql::Query(op) => {
                operation_type = Task::READ;
                query_string = op.into_msqlfinalstring().into_inner();
            }

            Msql::EndTx(op) => match op.mode() {
                MsqlEndTxMode::Commit => {
                    operation_type = Task::COMMIT;
                }
                MsqlEndTxMode::Rollback => {
                    operation_type = Task::ABORT;
                }
            },
        }

        QueueMessage {
            identifier: identifier,
            operation_type: operation_type,
            query: query_string,
            versions: versions,
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
            Err(_) => {
                result = "There was an error".to_string();
                succeed = false;
            }
        }

        let result_type;
        let mut contained_newer_versions: TxVN = Default::default();

        match self.operation_type {
            Task::BEGIN => {
                result_type = QueryResultType::BEGIN;
                match self.versions {
                    Some(versions) => {
                        contained_newer_versions = versions;
                    }
                    None => {}
                }
            }
            Task::READ | Task::WRITE => {
                result_type = QueryResultType::QUERY;
            }
            Task::COMMIT | Task::ABORT => {
                result_type = QueryResultType::END;
            }
        };

        QueryResult {
            identifier: self.identifier,
            result: result,
            result_type: result_type,
            succeed: succeed,
            contained_newer_versions: contained_newer_versions,
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
        self.queue.push(op);
        self.notify.notify_one();
    }

    pub fn get_notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    pub async fn get_all_version_ready_task(&mut self, version: &mut Arc<Mutex<DbVersion>>) -> Vec<QueueMessage> {
        let partitioned_queue: Vec<_> = stream::iter(self.queue.clone())
            .then(|op| async { (op.clone(), version.lock().await.violate_version(op.versions)) })
            .collect()
            .await;
        let (unready_ops, ready_ops): (Vec<_>, Vec<_>) =
            partitioned_queue.into_iter().partition(|(_, violate)| *violate);
        self.queue = unready_ops.into_iter().map(|(op, _)| op).collect();

        ready_ops.into_iter().map(|(op, _)| op).collect()
    }
}

pub struct DbVersion {
    db_version: DbVN,
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
        self.db_version
            .release_version(transaction_version.into_dbvn_release_request());
        self.notify.notify_one();
    }

    pub fn violate_version(&self, transaction_version: Option<TxVN>) -> bool {
        if let Some(tx_version) = transaction_version {
            return self.db_version.can_execute_query(&tx_version.txtablevns);
        }
        false
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
                let mut row: Vec<&str> = Vec::new();
                row.reserve(len);

                for index in 0..len {
                    row.push(query_row.get(index).unwrap());
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
}

#[derive(Serialize, Deserialize, Clone)]
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
    use crate::core::*;

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
    #[ignore]
    fn postgres_read_test() {
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
            let result = conn
                .simple_query("SELECT name, age, designation, salary FROM public.tbltest;")
                .await
                .unwrap();

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
    async fn pending_queue_task_order_test() {
        let mut dbversion = Arc::new(Mutex::new(DbVersion::new(Default::default())));
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
            Some(TxVN::default()),
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
            Some(TxVN::default()),
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

        let ready_tasks = queue.get_all_version_ready_task(&mut dbversion).await;

        ready_tasks.iter().for_each(|task| {
            println!("{}", task.identifier);
        });

        queue.queue.iter().for_each(|task| {
            println!("{}", task.identifier);
        });
    }

    #[tokio::test]
    async fn pending_queue_task_order_test_2() {
        let mut dbversion = Arc::new(Mutex::new(DbVersion::new(Default::default())));
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
            Some(TxVN::default()),
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
            Some(TxVN::default()),
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

        let ready_tasks = queue.get_all_version_ready_task(&mut dbversion).await;

        ready_tasks.iter().for_each(|task| {
            println!("{}", task.identifier);
        });

        queue.queue.iter().for_each(|task| {
            println!("{}", task.identifier);
        });

        println!("Number of tasks is: {}", ready_tasks.len());
        assert!(ready_tasks.len() == 2);
    }
}
