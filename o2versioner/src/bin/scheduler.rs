use actix_web::{web, App, HttpServer};
use o2versioner::scheduler::*;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    HttpServer::new(|| App::new().route("/", web::get().to(appserver_handler::greet)))
        .bind("127.0.0.1:8080")?
        .run()
        .await
}
