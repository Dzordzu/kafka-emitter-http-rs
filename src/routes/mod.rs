use actix_web::{Responder, get};

pub mod experiment;
pub mod measurements;
pub mod messages;

#[utoipa::path(
    tag = "internal",
    responses(
        (status = 200, body = String)
    )
)]
#[get("/healthz")]
async fn health() -> impl Responder {
    "OK"
}

#[utoipa::path(
    tag = "internal",
    responses(
        (status = 200, body = String)
    )
)]
#[get("/readyz")]
async fn ready() -> impl Responder {
    "OK"
}
