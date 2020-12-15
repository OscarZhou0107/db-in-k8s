//! Configurations for scheduler, sequencer and dbproxy
//!
//! # To set up the configuration
//! 1. Can be parsed from file by using `Config::from_file<S: Into<String>>(path: S)`
//! 2. Construct the structs directly
//! 3. Use builder style setter functions to modify them

use config;
use serde::Deserialize;
use std::net::SocketAddr;

/// Config for scheduler, sequencer and dbproxies
///
/// # Notes
/// `Config::dbproxy` represents a collection of `DbProxyConfig`, where
/// each is the config for the corresponding dbproxy instance
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default)]
pub struct Config {
    pub scheduler: SchedulerConfig,
    pub sequencer: SequencerConfig,
    pub dbproxy: Vec<DbProxyConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            scheduler: Default::default(),
            sequencer: Default::default(),
            dbproxy: Vec::new(),
        }
    }
}

impl Config {
    /// Load `Config` from file at `path`
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

/// Config for scheduler
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default)]
pub struct SchedulerConfig {
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

impl Default for SchedulerConfig {
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

impl SchedulerConfig {
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

/// Config for sequencer
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default)]
pub struct SequencerConfig {
    pub addr: String,
    pub max_connection: Option<u32>,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            addr: String::new(),
            max_connection: None,
        }
    }
}

impl SequencerConfig {
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

/// Config for a single dbproxy instance
#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
#[serde(default)]
pub struct DbProxyConfig {
    pub addr: String,
    /// If `None`, using mock_db
    pub sql_conf: Option<String>,
}

impl Default for DbProxyConfig {
    fn default() -> Self {
        Self {
            addr: String::new(),
            sql_conf: None,
        }
    }
}

impl DbProxyConfig {
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

    pub fn to_addr(&self) -> SocketAddr {
        self.addr.parse().expect("Invalid dbproxy addr")
    }
}

/// Unit test for `Config`
#[cfg(test)]
mod tests_config {
    use super::{Config, DbProxyConfig, SchedulerConfig, SequencerConfig};

    #[test]
    fn test_from_file() {
        let conf = Config::from_file("tests/config/conf1.toml");

        assert_eq!(
            conf,
            Config {
                scheduler: SchedulerConfig::new("127.0.0.1:1077")
                    .set_admin_addr(Option::<String>::None)
                    .set_max_connection(Some(50))
                    .set_sequencer_pool_size(20)
                    .set_dispatcher_queue_size(500)
                    .set_transceiver_queue_size(500)
                    .set_performance_logging(Some("./perf"))
                    .set_detailed_logging(Option::<String>::None)
                    .set_disable_early_release(false)
                    .set_disable_single_read_optimization(false),
                sequencer: SequencerConfig::new("127.0.0.1:9876").set_max_connection(Some(50)),
                dbproxy: vec![
                    DbProxyConfig::new("127.0.0.1:8876").set_sql_conf(Some(
                        "host=localhost port=5432 dbname=Test user=postgres password=Abc@123"
                    )),
                    DbProxyConfig::new("127.0.0.1:8876").set_sql_conf(Some(
                        "host=localhost port=5432 dbname=Test user=postgres password=Abc@123"
                    ))
                ]
            }
        );
    }
}
