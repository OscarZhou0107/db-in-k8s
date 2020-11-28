use super::msql::*;
use super::version_number::*;
use std::collections::HashMap;

/// Version number database for a single Sql database instance
pub struct VNDatabase(HashMap<String, VN>);

impl Default for VNDatabase {
    fn default() -> Self {
        Self(HashMap::new())
    }
}

impl VNDatabase {
    pub fn can_execute(&self, _tableops: &TableOps, _txvn: &TxVN) -> bool {
        todo!()
    }
}
