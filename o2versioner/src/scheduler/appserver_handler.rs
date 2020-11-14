use actix_web::{get, post, web, HttpMessage, HttpRequest, Responder};
use log::info;
use serde::Deserialize;

#[get("/")]
/// Handler for /
///
/// This GET request is for toy/experiment purposes
///
/// Test this handler on curl:
/// curl 127.0.0.1:8080
pub async fn greet(req: HttpRequest) -> impl Responder {
    info!(
        "Received request from {} on {}. cookies is {:?}. req is {:?}.",
        req.peer_addr().unwrap(),
        req.uri(),
        req.cookies(),
        req
    );

    format!(
        "Received request from {} on {}. cookies is {:?}. req is {:?}.",
        req.peer_addr().unwrap(),
        req.uri(),
        req.cookies(),
        req
    )
}

#[derive(Deserialize)]
pub struct SqlFormData {
    sql: String,
}

/// Handler for /sql
///
/// This POST request accepts one of the following SQL syntaxes:
/// 1. BEGIN {TRAN | TRANSACTION} [transaction_name] WITH MARK 'READ table_0 table_1 WRITE table_2' [;]
/// 2. UPDATE or SELECT
/// 3. COMMIT [{TRAN | TRANSACTION} [transaction_name]] [;]
///
/// {} - Keyword list
/// |  - Or
/// [] - Optional
///
/// Test this handler on curl:
/// curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -d "sql=hello" 127.0.0.1:8080/sql
#[post("/sql")]
pub async fn sql_handler(req: HttpRequest, sql_form: web::Form<SqlFormData>) -> impl Responder {
    info!(
        "Received request from {} on {}. sql_form is {}. cookies is {:?}. req is {:?}.",
        req.peer_addr().unwrap(),
        req.uri(),
        sql_form.sql,
        req.cookies(),
        req
    );

    format!(
        "Received request from {} on {}. sql_form is {}. cookies is {:?}. req is {:?}.",
        req.peer_addr().unwrap(),
        req.uri(),
        sql_form.sql,
        req.cookies(),
        req
    )
}
