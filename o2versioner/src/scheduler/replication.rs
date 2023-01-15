
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
pub async fn drop_connect(dbproxy_manager: Arc<RwLock<DbproxyManager>>, dbvn_manager:Arc<RwLock<DbVNManager>>, id:char) {
    let id_str =id.to_string();
    let mut replicate_toml = String::from("o2versioner/replicates.toml");
    replicate_toml.insert_str(22, &id_str);
    println!("{}", replicate_toml);
    let replicate_conf = Conf::from_file(replicate_toml);
    let replicate_proxy = replicate_conf.dbproxy.get(0).unwrap().clone();
    let address = &replicate_proxy.addr;
    let server: Vec<_> = address.to_socket_addrs().expect("Invalid sequencer addr").collect();
    println!("[Oscar] proxy ips: {:?}", server[0]); 
    let socket:SocketAddr = server[0];
    dbproxy_manager.write().await.remove(socket);
    dbvn_manager.write().await.remove(socket);
}
pub async fn connect_replica(dbproxy_manager: Arc<RwLock<DbproxyManager>>, dbvn_manager:Arc<RwLock<DbVNManager>>, id:char) {
    //read the default config
    let id_str =id.to_string();
    let mut replicate_toml = String::from("o2versioner/replicates.toml");
    replicate_toml.insert_str(22, &id_str);
    println!("{}", replicate_toml);
    let conf = Conf::from_file("o2versioner/conf.toml");
    
    //-------- Print the info of proxy and dbvn managers before insertion -------//
    println!("In scheduler's connect_replica");
    dbg!(dbproxy_manager.read().await.inner());
    dbg!(dbvn_manager.read().await.inner());

    // Obtain the IP address of the new proxy
    // All new proxy increment from the base port addr 38877, so we have a static variable here to keep track
    // static INDEX: AtomicUsize = AtomicUsize::new(0);
    //let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), IP_COUNTER.fetch_add(1, Ordering::Relaxed) as u16);
    let replicate_conf = Conf::from_file(replicate_toml);
    let replicate_proxy = replicate_conf.dbproxy.get(0).unwrap().clone();
    let address = &replicate_proxy.addr;
    let server: Vec<_> = address.to_socket_addrs().expect("Invalid sequencer addr").collect();
    println!("[Oscar] proxy ips: {:?}", server[0]); 
    let socket:SocketAddr = server[0];
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
    else {
        println!("Data migration completed!");
    }
    
    repdata(dbproxy_manager.clone(), dbvn_manager.clone(), id).await;

    // ----- wait for the thread ------//
    let _join = tokio::join!(transceiver_handle);
}

pub async fn repdata(dbproxy_manager: Arc<RwLock<DbproxyManager>>, dbvn_manager:Arc<RwLock<DbVNManager>>, id:char) {
    let id_str =id.to_string();
    let mut replicate_toml = String::from("o2versioner/replicates.toml");
    replicate_toml.insert_str(22, &id_str);

    let replicate_conf = Conf::from_file(replicate_toml);
    let replicate_proxy = replicate_conf.dbproxy.get(0).unwrap().clone();
    
    let address = &replicate_proxy.addr;
    let server: Vec<_> = address.to_socket_addrs().expect("Invalid sequencer addr").collect();
    println!("[Oscar] proxy ips: {:?}", server[0]); 
    let dbproxy_addr:SocketAddr = server[0];

    let replicate_conf = Conf::from_file("o2versioner/conf_dbproxy0.toml");
    let replicate_proxy = replicate_conf.dbproxy.get(0).unwrap().clone();
    let address = &replicate_proxy.addr;
    let server: Vec<_> = address.to_socket_addrs().expect("Invalid sequencer addr").collect();
    println!("[Oscar] proxy ips: {:?}", server[0]); 
    let old_db:SocketAddr = server[0];

    let transceiver_addr = dbproxy_manager.read().await.get(&dbproxy_addr);
    let old_db_vn = dbvn_manager.read().await.get(&old_db);
    let dbproxy_addrs = vec![(dbproxy_addr, transceiver_addr)];
    let msg = Message::ReplicateData(old_db_vn.clone());
    println!("In replicating {:?}", old_db_vn.clone());
    let num_dbproxy = dbproxy_addrs.len();
    let dbproxy_tasks_stream = stream::iter(dbproxy_addrs);
    // Send all requests to transceivers
    let _addr_receipts: Vec<_> = dbproxy_tasks_stream
    .then(move |(dbproxy_addr, transceiver_addr)| {
        let msg = msg.clone();
        let dbproxy_addr_clone = dbproxy_addr.clone();
        trace!("-> {:?}", msg);
        async move {
            transceiver_addr
                .request_nowait(TransceiverRequest::Dbreplica {
                    dbproxy_addr: dbproxy_addr.clone(),
                    dbproxy_msg: msg,
                })
                .inspect_err(|e| error!("Cannot send: {:?}", e))
                .map_ok(|receipt| (dbproxy_addr, receipt))
                .await
        }
        .instrument(info_span!("->dbproxy", N = num_dbproxy, message = %dbproxy_addr_clone))
    })
    .filter_map(|r| async move {
        match r {
            Err(_) => None,
            Ok(r) => Some(r),
        }
    })
    .collect()
    .await;
}