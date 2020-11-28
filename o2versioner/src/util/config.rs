use config;
use serde::Deserialize;

#[derive(Debug, Eq, PartialEq, Deserialize, Clone)]
pub struct Config {
    pub scheduler: SchedulerConfig,
    pub sequencer: SequencerConfig,
    pub dbproxy: Vec<DbProxyConfig>,
    pub dbschemas: Vec<DbSchemaConfig>,
}

impl Config {
    pub fn from_file(path: &str) -> Self {
        let mut source = config::Config::default();
        source.merge(config::File::with_name(path)).expect("Error");
        source.try_into().expect("Invalid Configuration format!")
    }
}

#[derive(Debug, Eq, PartialEq, Deserialize, Clone)]
pub struct SchedulerConfig {
    pub addr: String,
}

#[derive(Debug, Eq, PartialEq, Deserialize, Clone)]
pub struct SequencerConfig {
    pub addr: String,
}

#[derive(Debug, Eq, PartialEq, Deserialize, Clone)]
pub struct DbProxyConfig {
    pub addr: String,
    pub sql_addr: String,
}

#[derive(Debug, Eq, PartialEq, Deserialize, Clone)]
pub struct DbSchemaConfig {
    pub table_name: String,
}

/// Unit test for `Config`
#[cfg(test)]
mod tests_config {
    use super::{Config, DbProxyConfig, DbSchemaConfig, SchedulerConfig, SequencerConfig};

    #[test]
    fn test_from_file() {
        let conf = Config::from_file("tests/config/conf1.toml");

        assert_eq!(
            conf,
            Config {
                scheduler: SchedulerConfig {
                    addr: String::from("127.0.0.1:1077")
                },
                sequencer: SequencerConfig {
                    addr: String::from("127.0.0.1:9876")
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
                ],
                dbschemas: vec![
                    DbSchemaConfig {
                        table_name: String::from("table1")
                    },
                    DbSchemaConfig {
                        table_name: String::from("table2")
                    }
                ]
            }
        );
    }
}
