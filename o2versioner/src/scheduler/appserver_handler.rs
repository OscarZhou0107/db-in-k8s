use actix_web::{HttpRequest, Responder};
use log::info;

pub async fn greet(req: HttpRequest) -> impl Responder {
    info!("Received request from {}", req.peer_addr().unwrap());
    format!("Hello {:?}!", req)
}
