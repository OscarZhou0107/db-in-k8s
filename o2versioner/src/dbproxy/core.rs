use crate::core::{msql::Msql, operation::Operation as OperationType};
use crate::core::transaction_version::{TxTableVN, TxVN};
use futures::prelude::*;
use bb8_postgres::{PostgresConnectionManager, bb8::{ManageConnection, Pool, PooledConnection}};
use mysql_async::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::Notify;
//extern crate csv;
//use csv::Writer;
use tokio_postgres::{Client, Config, Connection, Error, NoTls, Socket, tls::NoTlsStream};
use async_trait::async_trait;


#[derive(Clone)]
pub struct PostgresSqlConnPool {
    pool : Pool<PostgresConnectionManager<NoTls>>,
}

impl PostgresSqlConnPool {
    pub async fn new(url : Config, max_conn : u32) ->Self {
        
        let manager = PostgresConnectionManager::new(url, NoTls);
        let pool = Pool::builder()
        .max_size(max_conn)
        .build(manager)
        .await
        .unwrap();

        Self {pool : pool}
    }

    pub async fn get_conn(&self) -> PooledConnection<'_, PostgresConnectionManager<NoTls>> {
        self.pool.get().await.unwrap()
    }
}

// pub struct ConnectionManager {
//     url : String
// }

// impl ConnectionManager {
//     pub fn new(url : String) -> Self {
//         Self {url : url}
//     }
// }

// #[async_trait]
// impl ManageConnection for ConnectionManager {
//     type Connection = (Client, Connection<Socket, NoTlsStream>);
//     type Error = Error;

//     async fn connect(&self) -> Result<Self::Connection, Self::Error> {
//         tokio_postgres::connect().await
//     }

//     async fn is_valid(&self, conn: &mut bb8_postgres::bb8::PooledConnection<'_, Self>) -> Result<(), Self::Error> {
//         match conn.0.simple_query("SELECT 1").await {
//             Ok(_) => Ok(()),
//             Err(err) => Err(err)
//         }
//     }

//     fn has_broken(&self, conn: &mut Self::Connection) -> bool {
//         conn.0.is_closed()
//     }
// }

#[derive(Clone)]
pub struct QueueMessage {
    pub identifier : String,
    pub operation_type : Task,
    pub query : String,
    pub versions : Option<TxVN>,
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


pub struct MySqlRepository {
    conn: mysql_async::Conn,
}

impl MySqlRepository {
    pub async fn new(pool: mysql_async::Pool) -> Self {
        let conn = pool.get_conn().await.unwrap();
        Self { conn: conn }
    }
}

#[async_trait]
impl Repository for MySqlRepository {

    async fn start_transaction(&mut self) {
        self.conn.query_drop("START TRANSACTION;").await.unwrap();
    }

    async fn execute_read(&mut self) -> QueryResult {
        let _ = self
            .conn
            .query_iter("INSERT INTO cats (name, owner, birth) VALUES ('haha2', 'haha3', CURDATE())")
            .await
            .unwrap();
        test_helper_get_query_result_non_release()
    }

    async fn execute_write(&mut self) -> QueryResult {
        let _ = self
            .conn
            .query_iter("INSERT INTO cats (name, owner, birth) VALUES ('haha2', 'haha3', CURDATE())")
            .await
            .unwrap();
        test_helper_get_query_result_non_release()
    }

    async fn commit(&mut self) -> QueryResult {
        self.conn.query_drop("COMMIT;").await.unwrap();
        test_helper_get_query_result_version_release()
    }

    async fn abort(&mut self) -> QueryResult {
        self.conn.query_drop("ROLLBACK;").await.unwrap();
        test_helper_get_query_result_version_release()
    }
}

// pub struct MySqlToCsvWriter {    
//     w : Writer<Vec>,
// }

// impl MySqlToCsvWriter {
//     pub fn new() -> Self {
//         Self {w : Writer::from_writer(vec![])}
//     }
// }

#[derive(Serialize, Deserialize, Clone)]
pub enum QueryResultType {
    BEGIN,
    QUERY,
    END,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct QueryResult {
    pub result : String,
    pub succeed : bool,
    pub result_type : QueryResultType,
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
        result : " ".to_string(),
        result_type : QueryResultType::BEGIN,
        succeed : true,
        contained_newer_versions : Vec::new(),
    }
}

fn test_helper_get_query_result_non_release() -> QueryResult {
    QueryResult {
        result : " ".to_string(),
        result_type : QueryResultType::END,
        succeed : true,
        contained_newer_versions : Vec::new(),
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

//     #[ignore]
//     #[tokio::test]
//     async fn test_sql_connection() {
//         let url = "mysql://root:Rayh8768@localhost:3306/test";
//         let pool = mysql_async::Pool::new(url);
//         match pool.get_conn().await {
//             Ok(_) => {
//                 println!("OK");
//             }
//             Err(e) => {
//                 println!("error is =============================== : {}", e);
//             }
//         }
//     }

//     #[ignore]
//     #[tokio::test]
//     async fn run_sql_query() {
//         let url = "mysql://root:Rayh8768@localhost:3306/test";
//         let pool = mysql_async::Pool::new(url);
//         //let mut wtr = csv::Writer::from_writer(io::stdout());

//         let mut conn = pool.get_conn().await.unwrap();

//         let mut raw = conn.query_iter("select * from cats").await.unwrap();
        
//         let mut results: Vec<mysql_async::Row> = raw.collect().await.unwrap();
//         results.iter_mut().for_each(|r| {
//             let len = r.len();
//             r.columns().iter().for_each(|c| {
//                 println!("col {}", c.name_str());
                
//             });

//             for i in 0..len {
//                 let val : String = r.take(i).unwrap();
//                 println!("len {}", val);
//             }
//         });
//     }

//     #[test]
//     fn postgres_test() {

//         let mut rt = tokio::runtime::Runtime::new().unwrap();

//         rt.block_on(async move {
//             let mut config = tokio_postgres::Config::new();
//             config.user("postgres");
//             config.password("Rayh8768");
//             config.host("localhost");
//             config.port(5432);

//             let manager = PostgresConnectionManager::new(config, NoTls);
//             let pool = Pool::builder()
//             .build(manager)
//             .await
//             .unwrap();

//             let conn = pool.get().await.unwrap();
//             conn.simple_query("INSERT INTO tblTest (name, age, designation, salary) VALUES ('haha', 100, 'Manager', 99999").await;
//             });
//     }


// }
