#![allow(dead_code)]
use crate::util::tcp::TcpStreamConnectionManager;
use bb8::Pool;
use futures::prelude::*;
use std::collections::HashMap;
use std::iter::Cycle;
use std::net::SocketAddr;
use std::vec::IntoIter;

pub struct DbproxyManager {
    dbproxies: HashMap<SocketAddr, Pool<TcpStreamConnectionManager>>,
    next_single_use_dbproxy: Cycle<IntoIter<SocketAddr>>,
}

impl DbproxyManager {
    /// Converts an `Iterator<Item = dbproxy_port: SocketAddr>` with `max_conn: u32` 'into `DbproxyManager`
    pub async fn from_iter<I>(iter: I, max_conn: u32) -> Self
    where
        I: IntoIterator<Item = SocketAddr>,
    {
        let dbproxies: HashMap<_, _> = stream::iter(iter)
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
            .await;

        let dbproxies_addrs: Vec<_> = dbproxies.keys().cloned().collect();
        let next_single_use_dbproxy = dbproxies_addrs.into_iter().cycle();
        DbproxyManager {
            dbproxies,
            next_single_use_dbproxy,
        }
    }

    pub fn get_single_dbproxy(&mut self) -> (SocketAddr, Pool<TcpStreamConnectionManager>) {
        self.dbproxies
            .get_key_value(&self.next_single_use_dbproxy.next().unwrap())
            .map(|(addr, pool)| (addr.clone(), pool.clone()))
            .unwrap()
    }

    pub fn get(&self) -> &HashMap<SocketAddr, Pool<TcpStreamConnectionManager>> {
        &self.dbproxies
    }
}
