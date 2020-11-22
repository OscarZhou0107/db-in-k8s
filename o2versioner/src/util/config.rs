#![allow(dead_code)]
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    sequencer: SequencerConfig,
    dbproxy: Vec<DbProxyConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SequencerConfig {
    addr: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbProxyConfig {
    addr: String,
}
