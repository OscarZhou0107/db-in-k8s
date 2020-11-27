use crate::core::sql::SqlString;
use std::convert::TryFrom;
use bytes::Bytes;

/// Represents a MySQL client/server protocol packet
///
/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/PAGE_PROTOCOL.html
///
/// # TODO:
/// 1. Uses bytes::Bytes to store bytes.
/// https://docs.rs/bytes/0.6.0/bytes/struct.Bytes.html
/// 2. Write converter functions to SqlString
/// 
/// # Supports:
/// 1. Connection Phase, SSL off, compression off. Bypassed
/// 2. Command Phase, Text Protocol. Deserialized and handled
pub struct Packet(Bytes);

impl TryFrom<Packet> for SqlString {
    type Error = ();

    /// Try to construct a `SqlString` from `Packet`
    fn try_from(_packet: Packet) -> Result<Self, Self::Error> {
        // Ok(SqlString(blablabla)) if successful; Err(()) otherwise
        todo!()
    }
}

/// Unit test for `Packet`
#[cfg(test)]
mod tests_packet {
    #![allow(warnings)]
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn test_try_into_sqlstring() {
        // let a = Packet(Bytes::from(static(b"blablabla")))
        // if let Ok(sqlstring) = SqlString::try_from(a) {
        //    println(sqlstring);
        // }
    }
}
