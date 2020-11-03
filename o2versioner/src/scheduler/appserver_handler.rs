use actix_web::{HttpRequest, Responder};

pub async fn greet(req: HttpRequest) -> impl Responder {
    format!("Hello {:?}!", req)
}
