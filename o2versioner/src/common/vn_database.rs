use super::version_number::VN;
use std::collections::HashMap;

#[allow(dead_code)]
/// Version number database for a single Sql database instance
pub type VNDatabase = HashMap<String, VN>;
