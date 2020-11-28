use crate::core::msql::Operation as OperationType;
use crate::core::version_number::{TableVN, TxVN};
use mysql_async::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

pub struct PendingQueue {
    pub queue: Vec<Operation>,
    pub notify: Arc<Notify>,
}

impl PendingQueue {
    pub fn new() -> Self {
        Self {
            queue: Vec::new(),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn push(&mut self, op: Operation) {
        self.queue.push(op);
        self.notify.notify();
    }

    pub fn get_notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }

    pub fn get_all_version_ready_task(&mut self, version: &mut Arc<Mutex<DbVersion>>) -> Vec<Operation> {
        let ready_ops = self
            .queue
            .split(|op| version.lock().unwrap().violate_version(op.clone()))
            .fold(Vec::new(), |mut acc_ops, ops| {
                ops.iter().for_each(|op| {
                    acc_ops.push(op.clone());
                });
                acc_ops
            });

        self.queue
            .retain(|op| version.lock().unwrap().violate_version(op.clone()));
        ready_ops
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
            .table_vns
            .iter()
            .for_each(|t| match self.table_versions.get_mut(&t.table) {
                Some(v) => *v = t.vn + 1,
                None => println!("Table {} not found to release version.", t.table),
            });
        self.notify.notify();
    }

    pub fn release_on_tables(&mut self, tables: Vec<TableVN>) {
        tables.iter().for_each(|t| match self.table_versions.get_mut(&t.table) {
            Some(v) => *v = t.vn + 1,
            None => println!("Table {} not found to release version.", t.table),
        });
        self.notify.notify();
    }

    pub fn violate_version(&self, transaction_version: Operation) -> bool {
        transaction_version.table_vns.iter().any(|t| {
            if let Some(v) = self.table_versions.get(&t.table) {
                return *v < t.vn;
            } else {
                return true;
            }
        })
    }

    pub fn get_notify(&self) -> Arc<Notify> {
        self.notify.clone()
    }
}

pub struct Repository {
    conn: mysql_async::Conn,
}

impl Repository {
    pub async fn new(pool: mysql_async::Pool) -> Self {
        let conn = pool.get_conn().await.unwrap();
        Self { conn: conn }
    }

    pub async fn start_transaction(&mut self) {
        self.conn.query_drop("START TRANSACTION;").await.unwrap();
    }

    pub async fn execute_read(&mut self) -> QueryResult {
        let _ = self
            .conn
            .query_iter("INSERT INTO cats (name, owner, birth) VALUES ('haha2', 'haha3', CURDATE())")
            .await
            .unwrap();
        test_helper_get_query_result_non_release()
    }

    pub async fn execute_write(&mut self) -> QueryResult {
        let _ = self
            .conn
            .query_iter("INSERT INTO cats (name, owner, birth) VALUES ('haha2', 'haha3', CURDATE())")
            .await
            .unwrap();
        test_helper_get_query_result_non_release()
    }

    pub async fn commit(&mut self) -> QueryResult {
        self.conn.query_drop("COMMIT;").await.unwrap();
        test_helper_get_query_result_version_release()
    }

    pub async fn abort(&mut self) -> QueryResult {
        self.conn.query_drop("ROLLBACK;").await.unwrap();
        test_helper_get_query_result_version_release()
    }
}

#[derive(Default, Serialize, Deserialize, Clone)]
pub struct QueryResult {
    pub result: String,
    pub version_release: bool,
    pub contained_newer_versions: Vec<TableVN>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Operation {
    pub transaction_id: String,
    pub task: Task,
    pub table_vns: Vec<TableVN>,
}

#[derive(Serialize, Deserialize, Clone)]
pub enum Task {
    READ,
    WRITE,
    ABORT,
    COMMIT,
}

fn test_helper_get_query_result_version_release() -> QueryResult {
    let mock_table_vs = vec![
        TableVN {
            table: "table1".to_string(),
            vn: 0,
            op: OperationType::R,
        },
        TableVN {
            table: "table2".to_string(),
            vn: 0,
            op: OperationType::R,
        },
    ];
    QueryResult {
        result: " ".to_string(),
        version_release: true,
        contained_newer_versions: mock_table_vs,
    }
}

fn test_helper_get_query_result_non_release() -> QueryResult {
    let mock_table_vs = vec![
        TableVN {
            table: "table1".to_string(),
            vn: 0,
            op: OperationType::R,
        },
        TableVN {
            table: "table2".to_string(),
            vn: 0,
            op: OperationType::R,
        },
    ];
    QueryResult {
        result: " ".to_string(),
        version_release: false,
        contained_newer_versions: mock_table_vs,
    }
}
//================================Test================================//

#[cfg(test)]
mod tests_dbproxy_core {
    use super::DbVersion;
    use super::Operation;
    use super::TableVN;
    use super::Task;
    use crate::core::msql::Operation as OperationType;
    use mysql_async::prelude::Queryable;
    use std::collections::HashMap;

    #[test]
    fn voilate_dbversion_should_return_true() {
        //Prepare
        let mut table_versions = HashMap::new();
        table_versions.insert("table1".to_string(), 0);
        table_versions.insert("table2".to_string(), 0);
        let db_version = DbVersion::new(table_versions);
        let versions = vec![
            TableVN {
                table: "table1".to_string(),
                vn: 0,
                op: OperationType::R,
            },
            TableVN {
                table: "table2".to_string(),
                vn: 1,
                op: OperationType::R,
            },
        ];
        let operation = Operation {
            table_vns: versions,
            transaction_id: "t1".to_string(),
            task: Task::READ,
        };
        //Action
        //Assert
        assert!(db_version.violate_version(operation));
    }

    #[test]
    fn obey_dbversion_should_return_false() {
        //Prepare
        let mut table_versions = HashMap::new();
        table_versions.insert("table1".to_string(), 0);
        table_versions.insert("table2".to_string(), 0);
        let db_version = DbVersion::new(table_versions);
        let versions = vec![
            TableVN {
                table: "table1".to_string(),
                vn: 0,
                op: OperationType::R,
            },
            TableVN {
                table: "table2".to_string(),
                vn: 0,
                op: OperationType::R,
            },
        ];
        let operation = Operation {
            table_vns: versions,
            transaction_id: "t1".to_string(),
            task: Task::READ,
        };
        //Action
        //Assert
        assert!(!db_version.violate_version(operation));
    }

    #[tokio::test]
    async fn test_sql_connection() {
        let url = "mysql://root:Rayh8768@localhost:3306/test";
        let pool = mysql_async::Pool::new(url);
        match pool.get_conn().await {
            Ok(_) => {
                println!("OK");
            }
            Err(e) => {
                println!("error is =============================== : {}", e);
            }
        }
    }

    #[ignore]
    #[tokio::test]
    async fn run_sql_query() {
        let url = "mysql://root:Rayh8768@localhost:3306/test";
        let pool = mysql_async::Pool::new(url);
        //let mut wtr = csv::Writer::from_writer(io::stdout());

        let mut conn = pool.get_conn().await.unwrap();

        let mut raw = conn.query_iter("select * from cats").await.unwrap();
        let results: Vec<mysql_async::Row> = raw.collect().await.unwrap();
        results.iter().for_each(|r| {
            println!("len {}", r.len());
        });
    }
}
