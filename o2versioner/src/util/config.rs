use config;
use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
pub struct Config {
    pub scheduler: SchedulerConfig,
    pub sequencer: SequencerConfig,
    pub dbproxy: Vec<DbProxyConfig>,
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
pub struct SchedulerConfig {
    pub addr: String,
    #[serde(default)]
    pub admin_addr: Option<String>,
    #[serde(default)]
    pub max_connection: Option<u32>,
    pub sequencer_pool_size: u32,
    pub dispatcher_queue_size: usize,
    pub transceiver_queue_size: usize,
    #[serde(default)]
    pub performance_logging: Option<String>,
    #[serde(default)]
    pub disable_early_release: bool,
}

impl SchedulerConfig {
    pub fn to_addr(&self) -> SocketAddr {
        self.addr.parse().expect("Invalid scheduler addr")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
pub struct SequencerConfig {
    pub addr: String,
    #[serde(default)]
    pub max_connection: Option<u32>,
}

impl SequencerConfig {
    pub fn to_addr(&self) -> SocketAddr {
        self.addr.parse().expect("Invalid sequencer addr")
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize)]
pub struct DbProxyConfig {
    pub addr: String,
    pub sql_conf: String,
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
                        sql_conf: "host=localhost port=5432 dbname=Test user=postgres password=Abc@123".to_string()
                    },
                    DbProxyConfig {
                        addr: String::from("127.0.0.1:8876"),
                        sql_conf: "host=localhost port=5432 dbname=Test user=postgres password=Abc@123".to_string()
                    }
                ]
            }
        );
    }
}
