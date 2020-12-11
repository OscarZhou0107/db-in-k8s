use config;
use serde::Deserialize;
use std::net::SocketAddr;

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
    pub fn from_file(path: &str) -> Self {
        let mut source = config::Config::default();
        source.merge(config::File::with_name(path)).expect("Error");
        source.try_into().expect("Invalid Configuration format!")
    }

    pub fn to_dbproxy_addrs(&self) -> Vec<SocketAddr> {
        self.dbproxy.iter().map(|dbproxyconf| dbproxyconf.to_addr()).collect()
    }
}

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
    pub disable_early_release: bool,
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
            performance_logging: None,
            disable_early_release: false,
        }
    }
}

impl SchedulerConfig {
    pub fn to_addr(&self) -> SocketAddr {
        self.addr.parse().expect("Invalid scheduler addr")
    }
}

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
    pub fn to_addr(&self) -> SocketAddr {
        self.addr.parse().expect("Invalid sequencer addr")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
pub struct DbProxyConfig {
    pub addr: String,
    /// If `None`, using mock_db
    #[serde(default)]
    pub sql_conf: Option<String>,
}

impl DbProxyConfig {
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
                scheduler: SchedulerConfig {
                    addr: String::from("127.0.0.1:1077"),
                    admin_addr: None,
                    max_connection: Some(50),
                    sequencer_pool_size: 20,
                    dispatcher_queue_size: 500,
                    transceiver_queue_size: 500,
                    performance_logging: None,
                    disable_early_release: false
                },
                sequencer: SequencerConfig {
                    addr: String::from("127.0.0.1:9876"),
                    max_connection: Some(50),
                },
                dbproxy: vec![
                    DbProxyConfig {
                        addr: String::from("127.0.0.1:8876"),
                        sql_conf: Some(
                            "host=localhost port=5432 dbname=Test user=postgres password=Abc@123".to_string()
                        ),
                    },
                    DbProxyConfig {
                        addr: String::from("127.0.0.1:8876"),
                        sql_conf: Some(
                            "host=localhost port=5432 dbname=Test user=postgres password=Abc@123".to_string()
                        ),
                    }
                ]
            }
        );
    }
}
