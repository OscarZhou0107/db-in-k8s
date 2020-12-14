//! Common utility functions

use chrono::Utc;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::path::Path;
use std::path::PathBuf;
use tracing::info;

pub fn remove_whitespace(s: &mut String) {
    s.retain(|c| !c.is_whitespace());
}

pub fn create_zip_writer<P: AsRef<Path>>(path: P) -> std::io::Result<GzEncoder<std::fs::File>> {
    std::fs::File::create(path).map(|f| GzEncoder::new(f, Compression::best()))
}

pub fn create_zip_csv_writer<P: AsRef<Path>>(path: P) -> std::io::Result<csv::Writer<GzEncoder<std::fs::File>>> {
    create_zip_writer(path).map(|w| csv::Writer::from_writer(w))
}

pub async fn prepare_logging_dir<P: AsRef<Path>>(dump_dir: P) -> PathBuf {
    let mut path_builder = PathBuf::from(dump_dir.as_ref());
    let log_dir_name = Utc::now().format("%y%m%d_%H%M%S").to_string();

    path_builder.push(log_dir_name);
    let cur_log_dir = path_builder.as_path();
    info!("Preparing {} for logging", cur_log_dir.display());
    tokio::fs::create_dir_all(cur_log_dir.clone()).await.unwrap();

    cur_log_dir.to_path_buf()
}
