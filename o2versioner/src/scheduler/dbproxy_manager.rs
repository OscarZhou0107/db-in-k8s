#![allow(dead_code)]
use crate::util::tcp::TcpStreamConnectionManager;
use bb8::Pool;
use futures::prelude::*;
use std::collections::HashMap;
use std::net::SocketAddr;

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

    pub fn get(&self) -> &HashMap<SocketAddr, Pool<TcpStreamConnectionManager>> {
        &self.0
    }
}
