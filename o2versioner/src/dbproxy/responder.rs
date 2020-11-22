use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use std::{sync::Mutex};
use std::sync::Arc;
use tokio::sync::Notify;
use super::core::{QueryResult, DbVersion};

pub struct Responder {}

impl Responder {
    pub fn run(mut receiver : mpsc::Receiver<QueryResult>, 
        semaphor : Arc<Semaphore>,
        version: Arc<Mutex<DbVersion>>,
        notify : Arc<Notify>
        ) {
        tokio::spawn(async move {
            while let Some(result) = receiver.recv().await {
                println!("rrrr");
                semaphor.add_permits(1);
                //Notify the dispatcher
                if result.version_release {
                    version.lock().unwrap().release_on_tables(result.contained_newer_versions);
                    notify.notify();
                }
                //TBD Reply to socket//
            }
        });
    }
}