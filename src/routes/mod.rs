use std::sync::atomic::Ordering;

use actix_web::{delete, get, web, Responder};

use crate::AppData;

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

#[utoipa::path(
    tag = "internal",
    responses(
        (status = 200, body = String)
    )
)]
/// In case of running job that cannot be stopped, too high memory usage etc.
#[delete("/restart-runtime")]
async fn restart_runtime(data: web::Data<AppData>) -> impl Responder {
    data.should_tokio_finish.store(false, Ordering::Relaxed);
    data.stop_handle.stop(true);
    "Restarting runtime..."
}

#[utoipa::path(
    tag = "internal",
    responses(
        (status = 200, body = String)
    )
)]
/// Kill it with fire!
#[delete("/stop-runtime")]
async fn stop_runtime(data: web::Data<AppData>) -> impl Responder {
    data.should_tokio_finish.store(true, Ordering::Relaxed);
    data.stop_handle.stop(true);
    "Stopping runtime..."
}
