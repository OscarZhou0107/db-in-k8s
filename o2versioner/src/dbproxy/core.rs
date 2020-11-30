use crate::core::transaction_version::{TxTableVN, TxVN};
use crate::core::{msql::Msql, operation::Operation as OperationType};
use bb8_postgres::{
    bb8::{ManageConnection, Pool, PooledConnection},
    PostgresConnectionManager,
};
use futures::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use async_trait::async_trait;
use csv::*;
use tokio_postgres::{tls::NoTlsStream, Client, Config, Connection, Error, NoTls, Socket};

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
    pub identifier: String,
    pub operation_type: Task,
    pub query: String,
    pub versions: Option<TxVN>,
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
    table_versions: HashMap<String, u64>,
    notify: Arc<Notify>,
}

impl DbVersion {
    pub fn new(table_versions: HashMap<String, u64>) -> Self {
        Self {
            table_versions: table_versions,
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn release_on_transaction(&mut self, transaction_version: TxVN) {
        transaction_version
            .txtablevns
            .iter()
            .for_each(|t| match self.table_versions.get_mut(&t.table) {
                Some(v) => *v = t.vn + 1,
                None => println!("Table {} not found to release version.", t.table),
            });
        self.notify.notify_one();
    }

    pub fn release_on_tables(&mut self, tables: Vec<TxTableVN>) {
        tables.iter().for_each(|t| match self.table_versions.get_mut(&t.table) {
            Some(v) => *v = t.vn + 1,
            None => println!("Table {} not found to release version.", t.table),
        });
        self.notify.notify_one();
    }

    pub fn violate_version(&self, transaction_version: Option<TxVN>) -> bool {
        if let Some(versions) = transaction_version {
            versions.txtablevns.iter().any(|t| {
                if let Some(v) = self.table_versions.get(&t.table) {
                    return *v < t.vn;
                } else {
                    return true;
                }
            });
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

    pub fn to_csv(mut self, message: Vec<tokio_postgres::SimpleQueryMessage>) -> String {
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
    pub result: String,
    pub succeed: bool,
    pub result_type: QueryResultType,
    pub contained_newer_versions: Vec<TxTableVN>,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum Task {
    BEGIN,
    READ,
    WRITE,
    ABORT,
    COMMIT,
}

fn test_helper_get_query_result_version_release() -> QueryResult {
    QueryResult {
        result: " ".to_string(),
        result_type: QueryResultType::BEGIN,
        succeed: true,
        contained_newer_versions: Vec::new(),
    }
}

fn test_helper_get_query_result_non_release() -> QueryResult {
    QueryResult {
        result: " ".to_string(),
        result_type: QueryResultType::END,
        succeed: true,
        contained_newer_versions: Vec::new(),
    }
}
//================================Test================================//

// #[cfg(test)]
// mod tests_dbproxy_core {
//     use super::DbVersion;
//     use super::Task;
//     use super::TxTableVN;
//     use crate::core::operation::Operation as OperationType;
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
//                 op: OperationType::R,
//             },
//             TxTableVN {
//                 table: "table2".to_string(),
//                 vn: 1,
//                 op: OperationType::R,
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
//                 op: OperationType::R,
//             },
//             TxTableVN {
//                 table: "table2".to_string(),
//                 vn: 0,
//                 op: OperationType::R,
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

#[test]
#[ignore]
fn postgres_write_test() {
    let mut rt = tokio::runtime::Runtime::new().unwrap();

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
        let result = conn.simple_query(
            "INSERT INTO tbltest (name, age, designation, salary) VALUES ('haha', 100, 'Manager', 99999)",
        )
        .await
        .unwrap();
        conn.simple_query("COMMIT;").await.unwrap();

        result.iter().for_each(|q_message| {
            match q_message {
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
            }
        });

        let writer = PostgreToCsvWriter::new(Task::WRITE);
        let csv = writer.to_csv(result);

        println!("Converted string is: {}", csv);
    });
}

#[test]
#[ignore]
fn postgres_read_test() {
    let mut rt = tokio::runtime::Runtime::new().unwrap();

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

        result.iter().for_each(|q_message| {
            match q_message {
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
            }
        });

        let writer = PostgreToCsvWriter::new(Task::READ);
        let csv = writer.to_csv(result);

        println!("Converted string is: {}", csv);

    });
}

//}
