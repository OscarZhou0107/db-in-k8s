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
    pub max_connection: Option<u32>,
    pub sequencer_pool_size: u32,
    pub dbproxy_pool_size: u32,
    pub dispatcher_queue_size: usize,
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
    pub sql_addr: String,
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
                    max_connection: Some(50),
                    sequencer_pool_size: 20,
                    dbproxy_pool_size: 10,
                    dispatcher_queue_size: 500
                },
                sequencer: SequencerConfig {
                    addr: String::from("127.0.0.1:9876"),
                    max_connection: Some(50),
                },
                dbproxy: vec![
                    DbProxyConfig {
                        addr: String::from("127.0.0.1:8876"),
                        sql_addr: String::from("mysql://root:Rayh8768@localhost:3306/test")
                    },
                    DbProxyConfig {
                        addr: String::from("127.0.0.1:8877"),
                        sql_addr: String::from("mysql://root:Rayh8768@localhost:3306/test")
                    }
                ]
            }
        );
    }
}
