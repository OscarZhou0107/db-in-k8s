use crate::common::sql::SqlStmt;
use actix_session::Session;
use actix_web::{get, post, web, HttpMessage, HttpRequest, Responder};
use log::info;

/// Handler for /
///
/// This GET request is for toy/experiment purposes
///
/// Test this handler on curl:
/// curl 127.0.0.1:8080
///
#[get("/")]
pub async fn greet(req: HttpRequest, session: Session) -> impl Responder {
    // access session data
    if let Some(count) = session.get::<i32>("counter").unwrap() {
        session.set("counter", count + 1).unwrap();
    } else {
        session.set("counter", 1).unwrap();
    }

    info!(
        "Received request from {} on {}. cookies is {:?}. count is {:?}. req is {:?}",
        req.peer_addr().unwrap(),
        req.uri(),
        req.cookies(),
        session.get::<i32>("counter").unwrap(),
        req
    );

    format!(
        "Received request from {} on {}. cookies is {:?}. count is {:?}. req is {:?}",
        req.peer_addr().unwrap(),
        req.uri(),
        req.cookies(),
        session.get::<i32>("counter").unwrap(),
        req
    )
}

/// Handler for /sql
///
/// This POST request accepts one of the following SQL syntaxes encoded via json in the format as `struct SqlData`.
/// 1. BEGIN {TRAN | TRANSACTION} [transaction_name] WITH MARK 'READ table_0 table_1 WRITE table_2' [;]
/// 2. UPDATE or SELECT
/// 3. COMMIT [{TRAN | TRANSACTION} [transaction_name]] [;]
///
/// {} - Keyword list
/// |  - Or
/// [] - Optional
///
/// Test this handler on curl:
/// curl -X POST -H "Content-Type: application/json" -d '"select * from students;"' 127.0.0.1:8080/sql
///
#[post("/sql")]
pub async fn sql_handler(
    req: HttpRequest,
    sql_stmt: web::Json<SqlStmt>,
    _session: Session,
) -> impl Responder {
    info!(
        "From '{}' on '{}' with '{:?}'. cookies is '{:?}'. req is {:?}.",
        req.peer_addr().unwrap(),
        req.uri(),
        sql_stmt,
        req.cookies(),
        req
    );

    format!(
        "From '{}' on '{}' with '{:?}'. cookies is '{:?}'. req is {:?}.",
        req.peer_addr().unwrap(),
        req.uri(),
        sql_stmt,
        req.cookies(),
        req
    )
}
