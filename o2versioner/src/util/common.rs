use flate2::write::GzEncoder;
use flate2::Compression;
use std::path::Path;

pub fn remove_whitespace(s: &mut String) {
    s.retain(|c| !c.is_whitespace());
}

pub fn create_zip_writer<P: AsRef<Path>>(path: P) -> std::io::Result<GzEncoder<std::fs::File>> {
    std::fs::File::create(path).map(|f| GzEncoder::new(f, Compression::best()))
}

pub fn create_zip_csv_writer<P: AsRef<Path>>(path: P) -> std::io::Result<csv::Writer<GzEncoder<std::fs::File>>> {
    create_zip_writer(path).map(|w| csv::Writer::from_writer(w))
}
