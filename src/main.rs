use jemallocator::Jemalloc;

#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub mod consumers;
pub mod models;
pub mod routes;
pub mod state;

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::UNIX_EPOCH,
};

use actix_web::{App, HttpServer, dev::ServerHandle, web};
use tokio::sync::Mutex;
use utoipa::openapi::tag::TagBuilder;
use utoipa_actix_web::{AppExt, scope};
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

use crate::{
    models::{Experiment, MessageEvent},
    state::MessageMapping,
};

pub const MESSAGE_UUID_HEADER: &str = "x-message-uuid";
pub const EXPERIMENT_UUID_HEADER: &str = "x-experiment-uuid";

pub const DEFAULT_BROKERS_ENV: &str = "DEFAULT_BROKERS";
pub const DEFAULT_TOPIC_ENV: &str = "DEFAULT_TOPIC";
pub const HOST_ENV: &str = "HTTP_HOST";
pub const APP_PORT_ENV: &str = "HTTP_PORT";

pub fn get_now_millis() -> u128 {
    std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Could not calculate time since unix epoch")
        .as_millis()
}

#[derive(Default, Debug)]
pub struct StopHandle {
    inner: parking_lot::Mutex<Option<ServerHandle>>,
}

impl StopHandle {
    /// Sets the server handle to stop.
    pub(crate) fn register(&self, handle: ServerHandle) {
        *self.inner.lock() = Some(handle);
    }

    /// Sends stop signal through contained server handle.
    pub(crate) fn stop(&self, graceful: bool) {
        #[allow(clippy::let_underscore_future)]
        let _ = self.inner.lock().as_ref().unwrap().stop(graceful);
    }
}

#[derive(Debug)]
pub struct AppData {
    pub app_state: Mutex<crate::state::State>,
    pub should_tokio_finish: Arc<AtomicBool>,
    pub stop_handle: StopHandle,
}

impl AppData {
    pub fn new(should_tokio_finish: Arc<AtomicBool>) -> Self {
        AppData {
            app_state: Mutex::new(crate::state::State::new()),
            stop_handle: StopHandle::default(),
            should_tokio_finish,
        }
    }
}

impl AppData {
    pub async fn experiment_related_data(
        &self,
        experiment_uuid: &Uuid,
    ) -> Option<(Experiment, MessageMapping, Vec<MessageEvent>)> {
        let experiment = {
            self.app_state
                .lock()
                .await
                .messages_state
                .lock()
                .await
                .experiments
                .get(experiment_uuid)
                .cloned()
        }?;

        let events: Vec<MessageEvent> = {
            self.app_state
                .lock()
                .await
                .messages_state
                .lock()
                .await
                .events
                .get(experiment_uuid)
                .cloned()
                .unwrap_or_default()
        };

        let messages = self
            .app_state
            .lock()
            .await
            .messages_state
            .lock()
            .await
            .messages
            .get(experiment_uuid)
            .cloned()
            .unwrap_or_default();

        Some((experiment, messages, events))
    }
}

fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt().init();
    loop {
        let should_tokio_finish = Arc::new(AtomicBool::new(true));
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(main_async(should_tokio_finish.clone()))?;

        if should_tokio_finish.load(Ordering::Relaxed) {
            return Ok(());
        }
    }
}

async fn main_async(should_tokio_finish: Arc<AtomicBool>) -> std::io::Result<()> {
    let app_data = web::Data::new(AppData::new(should_tokio_finish));
    let host = std::env::var(HOST_ENV).unwrap_or("0.0.0.0".into());
    let port = std::env::var(APP_PORT_ENV)
        .ok()
        .and_then(|x| str::parse(&x).ok())
        .unwrap_or(8080);

    let app_data_clone = app_data.clone();
    let srv = HttpServer::new(move || {
        let (app, mut api) = App::new()
            .app_data(app_data_clone.clone())
            .into_utoipa_app()
            .service(routes::ready)
            .service(routes::health)
            .service(
                scope::scope("/runtime")
                    .service(routes::restart_runtime)
                    .service(routes::stop_runtime),
            )
            .service(
                scope::scope("/experiment")
                    .service(routes::experiment::begin)
                    .service(routes::experiment::end)
                    .service(routes::experiment::reset)
                    .service(routes::experiment::restore)
                    .service(routes::experiment::get_insights)
                    .service(routes::experiment::get_config)
                    .service(routes::experiment::list_experiments),
            )
            .service(
                scope::scope("/message")
                    .service(routes::messages::send)
                    .service(routes::messages::send_job),
            )
            .service(
                scope::scope("/measurements")
                    .service(routes::measurements::kafka_latencies)
                    .service(routes::measurements::send_receive_latencies)
                    .service(routes::measurements::messaged_bytes_size),
            )
            .split_for_parts();

        api.info.title = "kafka-http-emitter-rs".into();
        api.info.description =
            Some("Service for loadtesting kafka, kafka-connector and kafka-replicator".into());

        api.info.contact = None;
        api.info.license = None;
        api.info.version = std::option_env!("CARGO_PKG_VERSION").unwrap().into();

        let experiments_tag = TagBuilder::new()
            .name("experiment")
            .description(Some(
                "Endpoints to manage performed experiments / measurements",
            ))
            .build();

        let messages_tag = TagBuilder::new()
            .name("messages")
            .description(Some("Endpoints to send messages for the experiments"))
            .build();

        let measure_tag = TagBuilder::new()
            .name("measurements")
            .description(Some("Endpoints to retrive statistical data from experiments events"))
            .build();

        api.tags = Some(vec![experiments_tag, messages_tag, measure_tag]);

        app.service(SwaggerUi::new("/docs/{_:.*}").url("/api-docs/openapi.json", api))
            .service(web::redirect("/docs", "/docs/"))
    })
    .bind((host, port))?
    .run();

    app_data.stop_handle.register(srv.handle());

    srv.await
}
