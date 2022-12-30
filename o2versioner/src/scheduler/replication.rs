
use futures::prelude::*;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::{TcpListener};
use tracing::{field, info, info_span, instrument, warn, Instrument, Span};
use super::core::DbVNManager;
use super::core::{DbproxyManager};
use crate::comm::scheduler_dbproxy::*;
//bringing in every crates from the handler, don't need them all tho
// use super::dispatcher::*;
use super::transceiver::*;
use crate::util::conf::*;
use crate::util::executor::Executor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use tokio::sync::{RwLock};
use std::sync::Arc;
use tracing::{error, trace};
use std::sync::atomic::{AtomicUsize, Ordering};
use run_script::ScriptOptions;
use std::net::ToSocketAddrs;

pub async fn connect_replica(dbproxy_manager: Arc<RwLock<DbproxyManager>>, dbvn_manager:Arc<RwLock<DbVNManager>>) {
    //read the default config
    let conf = Conf::from_file("o2versioner/conf.toml");
    
    //-------- Print the info of proxy and dbvn managers before insertion -------//
    println!("In scheduler's connect_replica");
    dbg!(dbproxy_manager.read().await.inner());
    dbg!(dbvn_manager.read().await.inner());

    // Obtain the IP address of the new proxy
    // All new proxy increment from the base port addr 38877, so we have a static variable here to keep track
    static INDEX: AtomicUsize = AtomicUsize::new(0);
    //let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), IP_COUNTER.fetch_add(1, Ordering::Relaxed) as u16);
    let replicate_conf = Conf::from_file("o2versioner/replicates.toml");
    let replicate_proxy = replicate_conf.dbproxy.get(INDEX.fetch_add(1, Ordering::Relaxed) as usize).unwrap().clone();
    let address = &replicate_proxy.addr;
    let server: Vec<_> = address.to_socket_addrs().expect("Invalid sequencer addr").collect();
    println!("[Oscar] proxy ips: {:?}", server[1]); 
    let socket:SocketAddr = server[1];
    // Construct the new tranceiver and insert the IP addr into the managers
    let (transceiver_addr, transceiver) = Transceiver::new(conf.scheduler.transceiver_queue_size, socket);
    dbproxy_manager.write().await.insert(socket, transceiver_addr);
    dbvn_manager.write().await.insert(socket);
    // Launch the transceiver 
    let transceiver_handle = tokio::spawn(Box::new(transceiver).run().in_current_span());

    //------ compare the before and after of the managers -------//
    dbg!(dbproxy_manager.read().await.inner());
    dbg!(dbvn_manager.read().await.inner());

    //------ replicate data using pgdump -------//
    //pg_dump tpcw > tpcw_dump.sql 
    //psql tcpw3 < tpcw_dump.sql 
    let (code, output, error) = run_script::run_script!(
        r#"
            pg_dump tpcw > tpcw_dump.sql
            psql tpcw3 < tpcw_dump.sql
            "#
    )
    .unwrap();

    if code!=0 {
        println!("{}", output);
        println!("{}", error);
    }

    // ----- wait for the thread ------//
    let _join = tokio::join!(transceiver_handle);
}