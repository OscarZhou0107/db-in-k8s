//! Configurations for scheduler, sequencer and dbproxy
//!
//! # To set up the configuration
//! 1. Can be parsed from file by using `Config::from_file<S: Into<String>>(path: S)`
//! 2. Construct the structs directly
//! 3. Use builder style setter functions to modify them

use config;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::SocketAddr;

/// Config for scheduler, sequencer and dbproxies
///
/// # Notes
/// `Config::dbproxy` represents a collection of `DbProxyConfig`, where
/// each is the config for the corresponding dbproxy instance
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Conf {
    pub scheduler: SchedulerConf,
    pub sequencer: SequencerConf,
    pub dbproxy: Vec<DbProxyConf>,
}

impl Default for Conf {
    fn default() -> Self {
        Self {
            scheduler: Default::default(),
            sequencer: Default::default(),
            dbproxy: Vec::new(),
        }
    }
}

impl Conf {
    /// Load `Conf` from file at `path`
    pub fn from_file<S: Into<String>>(path: S) -> Self {
        let mut source = config::Config::default();
        source
            .merge(config::File::with_name(path.into().as_ref()))
            .expect("Error");
        source.try_into().expect("Invalid Configuration format!")
    }

    /// Return a list of dbproxy addresses in `Vec<SocketAddr>`
    pub fn to_dbproxy_addrs(&self) -> Vec<SocketAddr> {
        self.dbproxy.iter().map(|dbproxyconf| dbproxyconf.to_addr()).collect()
    }
}

/// Conf for scheduler
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerConf {
    pub addr: String,
    pub admin_addr: Option<String>,
    pub max_connection: Option<u32>,
    pub sequencer_pool_size: u32,
    pub dispatcher_queue_size: usize,
    pub transceiver_queue_size: usize,
    pub performance_logging: Option<String>,
    pub detailed_logging: Option<String>,
    pub disable_early_release: bool,
    pub disable_single_read_optimization: bool,
}

impl Default for SchedulerConf {
    fn default() -> Self {
        Self {
            addr: String::new(),
            admin_addr: None,
            max_connection: None,
            sequencer_pool_size: 10,
            dispatcher_queue_size: 500,
            transceiver_queue_size: 500,
            performance_logging: Some("./perf".to_string()),
            detailed_logging: None,
            disable_early_release: false,
            disable_single_read_optimization: false,
        }
    }
}

impl SchedulerConf {
    pub fn new<S: Into<String>>(addr: S) -> Self {
        Self::default().set_addr(addr)
    }

    pub fn set_addr<S: Into<String>>(mut self, addr: S) -> Self {
        self.addr = addr.into();
        self
    }

    pub fn set_admin_addr<S: Into<String>>(mut self, admin_addr: Option<S>) -> Self {
        self.admin_addr = admin_addr.map(|s| s.into());
        self
    }

    pub fn set_max_connection(mut self, max_connection: Option<u32>) -> Self {
        self.max_connection = max_connection;
        self
    }

    pub fn set_sequencer_pool_size(mut self, sequencer_pool_size: u32) -> Self {
        self.sequencer_pool_size = sequencer_pool_size;
        self
    }

    pub fn set_dispatcher_queue_size(mut self, dispatcher_queue_size: usize) -> Self {
        self.dispatcher_queue_size = dispatcher_queue_size;
        self
    }

    pub fn set_transceiver_queue_size(mut self, transceiver_queue_size: usize) -> Self {
        self.transceiver_queue_size = transceiver_queue_size;
        self
    }

    pub fn set_performance_logging<S: Into<String>>(mut self, performance_logging: Option<S>) -> Self {
        self.performance_logging = performance_logging.map(|s| s.into());
        self
    }

    pub fn set_detailed_logging<S: Into<String>>(mut self, detailed_logging: Option<S>) -> Self {
        self.detailed_logging = detailed_logging.map(|s| s.into());
        self
    }

    pub fn set_disable_early_release(mut self, disable_early_release: bool) -> Self {
        self.disable_early_release = disable_early_release;
        self
    }

    pub fn set_disable_single_read_optimization(mut self, disable_single_read_optimization: bool) -> Self {
        self.disable_single_read_optimization = disable_single_read_optimization;
        self
    }

    pub fn to_addr(&self) -> SocketAddr {
        self.addr.parse().expect("Invalid scheduler addr")
    }
}

/// Conf for sequencer
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct SequencerConf {
    pub addr: String,
    pub max_connection: Option<u32>,
}

impl Default for SequencerConf {
    fn default() -> Self {
        Self {
            addr: String::new(),
            max_connection: None,
        }
    }
}

impl SequencerConf {
    pub fn new<S: Into<String>>(addr: S) -> Self {
        Self::default().set_addr(addr)
    }

    pub fn set_addr<S: Into<String>>(mut self, addr: S) -> Self {
        self.addr = addr.into();
        self
    }

    pub fn set_max_connection(mut self, max_connection: Option<u32>) -> Self {
        self.max_connection = max_connection;
        self
    }

    pub fn to_addr(&self) -> SocketAddr {
        self.addr.parse().expect("Invalid sequencer addr")
    }
}

/// Conf for a single dbproxy instance
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DbProxyConf {
    pub addr: String,
    /// If `None`, using mock_db
    pub sql_conf: Option<String>,
    /// By default, None
    /// DbMockLatency also has default distribution of ~N (0, 0), 100% 0ms
    pub db_mock_latency: Option<DbMockLatency>,
}

impl Default for DbProxyConf {
    fn default() -> Self {
        Self {
            addr: String::new(),
            sql_conf: None,
            db_mock_latency: None,
        }
    }
}

impl DbProxyConf {
    pub fn new<S: Into<String>>(addr: S) -> Self {
        Self::default().set_addr(addr)
    }

    pub fn set_addr<S: Into<String>>(mut self, addr: S) -> Self {
        self.addr = addr.into();
        self
    }

    pub fn set_sql_conf<S: Into<String>>(mut self, sql_conf: Option<S>) -> Self {
        self.sql_conf = sql_conf.map(|s| s.into());
        self
    }

    pub fn set_db_mock_latency(mut self, db_mock_latency: Option<DbMockLatency>) -> Self {
        self.db_mock_latency = db_mock_latency;
        self
    }

    pub fn to_addr(&self) -> SocketAddr {
        self.addr.parse().expect("Invalid dbproxy addr")
    }
}

/// In units of us, by default, 100% 0us
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct DbMockLatency {
    pub begintx: LatencyDistr,
    pub read: LatencyDistr,
    pub write: LatencyDistr,
    pub endtx: LatencyDistr,
}

impl Default for DbMockLatency {
    fn default() -> Self {
        Self {
            begintx: Default::default(),
            read: Default::default(),
            write: Default::default(),
            endtx: Default::default(),
        }
    }
}

impl DbMockLatency {
    pub fn set_begintx(mut self, begintx: LatencyDistr) -> Self {
        self.begintx = begintx;
        self
    }

    pub fn set_read(mut self, read: LatencyDistr) -> Self {
        self.read = read;
        self
    }

    pub fn set_write(mut self, write: LatencyDistr) -> Self {
        self.write = write;
        self
    }

    pub fn set_endtx(mut self, endtx: LatencyDistr) -> Self {
        self.endtx = endtx;
        self
    }
}

/// In units of us, by default, 100% 0ms
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct LatencyDistr {
    pub mean: u32,
    pub stddev: u32,
}

impl Default for LatencyDistr {
    fn default() -> Self {
        Self { mean: 0, stddev: 0 }
    }
}

impl LatencyDistr {
    /// In units of us
    pub fn new(mean: u32, stddev: u32) -> Self {
        Self { mean, stddev }
    }
}

impl fmt::Display for LatencyDistr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Latency(us) ~ N({}, {}**2)", self.mean, self.stddev)
    }
}

impl fmt::Debug for LatencyDistr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// Unit test for `Conf`
#[cfg(test)]
mod tests_conf {
    use super::*;

    #[test]
    fn test_from_file() {
        let conf = Conf::from_file("tests/conf/conf1.toml");

        assert_eq!(
            conf,
            Conf {
                scheduler: SchedulerConf::new("127.0.0.1:1077")
                    .set_admin_addr(Option::<String>::None)
                    .set_max_connection(Some(50))
                    .set_sequencer_pool_size(20)
                    .set_dispatcher_queue_size(500)
                    .set_transceiver_queue_size(500)
                    .set_performance_logging(Some("./perf"))
                    .set_detailed_logging(Option::<String>::None)
                    .set_disable_early_release(false)
                    .set_disable_single_read_optimization(false),
                sequencer: SequencerConf::new("127.0.0.1:9876").set_max_connection(Some(50)),
                dbproxy: vec![
                    DbProxyConf::new("127.0.0.1:8876")
                        .set_sql_conf(Some(
                            "host=localhost port=5432 dbname=Test user=postgres password=Abc@123"
                        ))
                        .set_db_mock_latency(Some(DbMockLatency::default().set_begintx(LatencyDistr::new(10, 1)))),
                    DbProxyConf::new("127.0.0.1:8877").set_sql_conf(Some(
                        "host=localhost port=5432 dbname=Test user=postgres password=Abc@123"
                    ))
                ]
            }
        );
    }
}
