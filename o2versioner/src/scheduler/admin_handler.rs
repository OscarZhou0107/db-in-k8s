use futures::prelude::*;
use tokio::io::AsyncBufReadExt;
use tokio::io::AsyncWriteExt;
use tokio::io::BufReader;
use tokio::net::{TcpListener, ToSocketAddrs};
use tracing::{field, info, info_span, instrument, warn, Instrument, Span};
use super::core::DbVNManager;
use super::core::{DbproxyManager};
use crate::comm::scheduler_dbproxy::*;
//bringing in every crates from the handler, don't need them all tho
// use super::dispatcher::*;
use super::transceiver::*;
use crate::util::conf::*;
use crate::util::executor::Executor;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs as otherToSocketAddrs};
use tokio::sync::{RwLock};
use std::sync::Arc;
use tracing::{error, trace};
use std::sync::atomic::{AtomicUsize, Ordering};
use run_script::ScriptOptions;
use super::replication::*;
use std::{thread, time};
/* 
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
    let server: Vec<_> = address<ToSocketAddrs>.to_socket_addrs().expect("Invalid sequencer addr").collect();
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
    else {
        println!("Data migration completed!");
    }

    repdata(dbproxy_manager.clone(), dbvn_manager.clone()).await;

    // ----- wait for the thread ------//
    let _join = tokio::join!(transceiver_handle);
}
*/
pub async fn replica(dbproxy_manager: Arc<RwLock<DbproxyManager>>, _dbvn_manager:Arc<RwLock<DbVNManager>>) {
    let dbproxy_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 38875);
    let new_db = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 38877);
    let transceiver_addr = dbproxy_manager.read().await.get(&dbproxy_addr);
    let dbproxy_addrs = vec![(dbproxy_addr, transceiver_addr)];
    let msg = Message::ReplicateRequest(new_db.clone());
    println!("In replicating {:?}", dbproxy_addrs.clone());
    println!("Msg {:?}", msg.clone());
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

// pub async fn repdata(dbproxy_manager: Arc<RwLock<DbproxyManager>>, dbvn_manager:Arc<RwLock<DbVNManager>>) {
//     static INDEX: AtomicUsize = AtomicUsize::new(0);
//     //let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), IP_COUNTER.fetch_add(1, Ordering::Relaxed) as u16);
//     let replicate_conf = Conf::from_file("o2versioner/replicates.toml");
//     let replicate_proxy = replicate_conf.dbproxy.get(INDEX.fetch_add(1, Ordering::Relaxed) as usize).unwrap().clone();
//     let dbproxy_addr:SocketAddr = replicate_proxy.addr.parse().expect("Unable to parse socket address");
//     let old_db = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 38875);
//     let transceiver_addr = dbproxy_manager.read().await.get(&dbproxy_addr);
//     let old_db_vn = dbvn_manager.read().await.get(&old_db);
//     let dbproxy_addrs = vec![(dbproxy_addr, transceiver_addr)];
//     let msg = Message::ReplicateData(old_db_vn.clone());
//     println!("In replicating {:?}", old_db_vn.clone());
//     let num_dbproxy = dbproxy_addrs.len();
//     let dbproxy_tasks_stream = stream::iter(dbproxy_addrs);
//     // Send all requests to transceivers
//     let _addr_receipts: Vec<_> = dbproxy_tasks_stream
//     .then(move |(dbproxy_addr, transceiver_addr)| {
//         let msg = msg.clone();
//         let dbproxy_addr_clone = dbproxy_addr.clone();
//         trace!("-> {:?}", msg);
//         async move {
//             transceiver_addr
//                 .request_nowait(TransceiverRequest::Dbreplica {
//                     dbproxy_addr: dbproxy_addr.clone(),
//                     dbproxy_msg: msg,
//                 })
//                 .inspect_err(|e| error!("Cannot send: {:?}", e))
//                 .map_ok(|receipt| (dbproxy_addr, receipt))
//                 .await
//         }
//         .instrument(info_span!("->dbproxy", N = num_dbproxy, message = %dbproxy_addr_clone))
//     })
//     .filter_map(|r| async move {
//         match r {
//             Err(_) => None,
//             Ok(r) => Some(r),
//         }
//     })
//     .collect()
//     .await;
// }

/// Helper function to bind to a `TcpListener` as an admin port and forward all incomming `TcpStream` to `connection_handler`.
///
/// # Notes:
/// 1. `addr` is the tcp port to bind to
/// 2. `admin_command_handler` is a `FnMut` closure takes in `String` and returns `Future<Output = (String, bool)>`,
/// with `String` represents the reply response, and `bool` denotes whether to continue the `TcpListener`.
/// 3. The returned `String` should not have any newline characters
#[instrument(name="listen", skip(addr, admin_command_handler, dbproxy_manager, dbvn_manager), fields(message=field::Empty))]
pub async fn start_admin_tcplistener<A, C, Fut>(addr: A, dbproxy_manager: Arc<RwLock<DbproxyManager>>, dbvn_manager:Arc<RwLock<DbVNManager>>,
                                                mut admin_command_handler: C)
where
    A: ToSocketAddrs,
    C: FnMut(String) -> Fut,
    Fut: Future<Output = (String, bool)> + Send + 'static,
{
    let mut listener = TcpListener::bind(addr).await.unwrap();
    let local_addr = listener.local_addr().unwrap();

    Span::current().record("message", &&local_addr.to_string()[..]);
    info!("Successfully binded");

    'outer: while let Some(tcp_stream) = listener.next().await {
        match tcp_stream {
            Ok(mut tcp_stream) => {
                let peer_addr = tcp_stream.peer_addr().unwrap();
                info!("Admin incomming connection [{}] established", peer_addr);

                let (tcp_read, mut tcp_write) = tcp_stream.split();
                let mut line_reader = BufReader::new(tcp_read).lines();
                while let Ok(line) = line_reader.try_next().await {
                    if let Some(line) = line {
                        let line = line.trim().to_owned();
                        
                        //handler for replicating tracievers 
                        if line.contains("connect") {
                            println!("{}", line);
                            let id = line.chars().nth(8).unwrap();
                            println!("{}", id);

                            info!("Starting a tranciever thread in the admin control pannel");
                            //[Larry] we start the above connect_replica() function in a thread 
                            connect_replica(dbproxy_manager.clone(), dbvn_manager.clone(), id).in_current_span().await;
                            
                            //now we need to update the proxy manager struct, so that the dispatcher is aware of the new proxy
                        }
                        else if line.contains("drop") {
                            println!("{}", line);
                            let ip: String = line[5..].trim().to_string();
                            println!("{}", ip);

                            info!("Drop a db proxy from scheduler");
                            //[Larry] we start the above connect_replica() function in a thread 
                            tokio::spawn(drop_connect(dbproxy_manager.clone(), dbvn_manager.clone(), ip).in_current_span()); 
                            
                            //now we need to update the proxy manager struct, so that the dispatcher is aware of the new proxy
                        }
                        else if line == "break" {
                            info!("Terminating admin connection from {:?}...", peer_addr);
                            break;
                        }
                        else {
                            //old admin command handlers 
                            let conn = async {
                                let (mut res, should_continue) = admin_command_handler(line.clone()).await;
                                assert!(
                                    !res.contains("\n"),
                                    "admin_command_handler reply message should not contain any newline characters"
                                );
                                res += "\n";
                                
                                // Reply block only after all outstanding txn are finished in queue
                                if line == "block" {
                                    let replicate_toml = String::from("o2versioner/conf_dbproxy0.toml");
                                    let replicate_conf = Conf::from_file(replicate_toml);
                                    let replicate_proxy = replicate_conf.dbproxy.get(0).unwrap().clone();
                                    let address = &replicate_proxy.addr;
                                    let server: Vec<_> = address.to_socket_addrs().expect("Invalid sequencer addr").collect();
                                    let dbproxy_addr:SocketAddr = server[0];

                                    thread::sleep(time::Duration::from_millis(100));
                                    loop {
                                        let old_db_vn = dbvn_manager.read().await.get(&dbproxy_addr);
                                        thread::sleep(time::Duration::from_millis(300));
                                        let new_db_vn = dbvn_manager.read().await.get(&dbproxy_addr);
                                        // Compare vn number to check if there is new transaction being proceeded
                                        if (new_db_vn.get_version_sum() == old_db_vn.get_version_sum()) {
                                            break;
                                        }
                                    }
                                    
                                }

                                
                                tcp_write
                                    .write_all(res.as_bytes())
                                    .map_ok_or_else(
                                        |e| warn!("-> Unable to reply message {}: {:?}", res.trim(), e),
                                        |_| info!("-> {}", res.trim()),
                                    )
                                    .await;

                                should_continue
                            };

                            let should_continue = conn
                                .instrument(info_span!("request", message = %peer_addr, cmd = &&line[..]))
                                .await;

                            if !should_continue {
                                break 'outer;
                            }
                        }

                    }
                }
            }
            Err(e) => {
                warn!("Cannot get client: {:?}", e);
            }
        }
    }
    // futures::future::join_all(tranceivers).await;
    warn!("Service terminated, have a good night");
}

// /// Unit test for `start_admin_tcplistener`
// #[cfg(test)]
// mod tests_start_admin_tcplistener {
//     use super::*;
//     use crate::util::tests_helper::*;
//     use std::iter::FromIterator;
//     use tokio::net::TcpStream;
//     use unicase::UniCase;

//     #[tokio::test]
//     async fn test_admin_tcplistener() {
//         let _guard = init_logger();

//         let admin_addr = "127.0.0.1:27643";

//         let admin_handle = tokio::spawn(start_admin_tcplistener(admin_addr, |msg| async {
//             let command = UniCase::new(msg);
//             if Vec::from_iter(vec!["kill", "exit", "quit"].into_iter().map(|s| s.into())).contains(&command) {
//                 (String::from("Terminating"), false)
//             } else {
//                 (format!("Unknown command: {}", command), true)
//             }
//         }));
//         let client_handle = tokio::spawn(async move {
//             let mut tcp_stream = TcpStream::connect(admin_addr).await.unwrap();
//             let res = mock_ascii_client(&mut tcp_stream, vec!["help", "exit"]).await;
//             info!("All responses received: {:?}", res);
//         });

//         tokio::try_join!(admin_handle, client_handle).unwrap();
//     }
// }
