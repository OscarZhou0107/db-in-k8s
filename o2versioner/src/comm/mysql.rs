/// Represents a MySQL client/server protocol packet
///
/// https://dev.mysql.com/doc/dev/mysql-server/8.0.12/PAGE_PROTOCOL.html
///
/// Supports:
/// 1. Connection Phase, SSL off, compression off. Bypassed
/// 2. Command Phase, Text Protocol. Deserialized and handled
pub struct Packet();
