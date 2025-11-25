use std::sync::Arc;

use actix_web::{Responder, http::StatusCode, post, web};
use rand::Rng;
use rdkafka::{
    ClientConfig,
    message::{Header, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
};

use crate::{
    AppData, EXPERIMENT_UUID_HEADER, MESSAGE_UUID_HEADER, get_now_millis,
    models::{EventType, Message, MessageEvent, SendMessage, SentMessage},
};

const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                        abcdefghijklmnopqrstuvwxyz\
                        0123456789";

fn handle_message_delivery_failure(message: &mut SentMessage, bytes_unsent: usize) {
    message.delivery_failures += 1;
    message.total_sent_bytes -= bytes_unsent;
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum ResponseError {
    #[error("No messages have been delivered. See logs for details")]
    NoMessagesDelivered,

    #[error("Could not find experiment with the provided uuid")]
    ExperimentNotFound,
}
impl actix_web::error::ResponseError for ResponseError {
    fn status_code(&self) -> StatusCode {
        match *self {
            Self::ExperimentNotFound => StatusCode::NOT_FOUND,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[utoipa::path(
    tag = "messages",
    responses(
        (status = 200, description = "Sent new messages", body = SentMessage),
        (status = 404, description = "Experiment not found"),
        (status = 207, description = "Some messages were ok, some failed", body = SentMessage),
        (status = 500 , description = "No message has been sent properly")
    )
)]
#[post("/")]
/// Send a new message
async fn send(
    params: web::Json<SendMessage>,
    data: web::Data<AppData>,
) -> actix_web::Result<impl Responder, ResponseError> {
    let params = params.into_inner();

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", params.brokers.clone())
        .set("queue.buffering.max.ms", params.buffering_ms.to_string())
        .set(
            "message.timeout.ms",
            params.message_timeout.0.as_millis().to_string(),
        );

    if params.body_size.as_bytes() > 512000 {

    }

    if params.ssl {
        config.set("security.protocol", "ssl");
        config.set("enable.ssl.certificate.verification", "false");
    }

    let producer: FutureProducer = config.create().expect("Producer creation failed");

    let mut rng = rand::rng();
    let payload: String = (0..params.body_size.as_bytes())
        .map(|_| {
            let idx = rng.random_range(0..CHARSET.len());
            char::from(CHARSET[idx])
        })
        .collect();

    {
        let state = data.app_state.lock().await.messages_state.clone();
        let state = state.lock().await;

        if !state.experiments.contains_key(&params.experiment_uuid) {
            return Err(ResponseError::ExperimentNotFound);
        }
    }

    // This loop is non blocking: all messages will be sent one after the other, without waiting
    // for the results.
    let messages_state = data.app_state.lock().await.messages_state.clone();
    let futures = (0..params.messages_number)
        .map(|i| {
            (
                i,
                params.clone(),
                messages_state.clone(),
                payload.clone(),
                producer.clone(),
            )
        })
        .map(
            |(i, params, messages_state, payload, producer)| async move {
                let message_uuid = uuid::Uuid::new_v4();
                // The send operation on the topic returns a future, which will be
                // completed once the result or failure from Kafka is received.
                let delivery_status = producer
                    .send(
                        FutureRecord::to(&params.topic)
                            .payload(payload.as_str())
                            .key(&format!("msg-{}", i))
                            .headers(
                                OwnedHeaders::new()
                                    .insert(Header {
                                        key: MESSAGE_UUID_HEADER,
                                        value: Some(&message_uuid.to_string()),
                                    })
                                    .insert(Header {
                                        key: EXPERIMENT_UUID_HEADER,
                                        value: Some(&params.experiment_uuid.to_string()),
                                    }),
                            ),
                        params.message_timeout.0,
                    )
                    .await;
                let now = get_now_millis();

                let mut state = messages_state.lock().await;

                state.insert_message(
                    Message {
                        uuid: message_uuid,
                        bytes_size: bytesize::ByteSize::b(payload.len() as u64).into(),
                    },
                    params.experiment_uuid,
                );

                let events = state.events.get_mut(&params.experiment_uuid).unwrap();

                events.push(MessageEvent {
                    message_uuid,
                    timestamp_millis: now,
                    topic: params.topic.clone(),
                    brokers: params.brokers.clone(),
                    event_type: EventType::Sent,
                });

                delivery_status
            },
        )
        .collect::<Vec<_>>();

    let total_bytes = payload.len() * params.messages_number;

    let mut message = SentMessage {
        experiment_uuid: params.experiment_uuid,
        bytes_size: payload.len(),
        message_number: params.messages_number,
        total_sent_bytes: total_bytes,
        total_sent_bytes_human_readable: bytesize::ByteSize::b(total_bytes as u64).into(),
        delivery_failures: 0,
    };

    if params.blocking {
        for future in futures {
            if let Err(e) = future.await {
                tracing::warn!("Failed to deliver message {:?}. Reason: {:?}", &message, e.0);
                handle_message_delivery_failure(&mut message, payload.len());
            }
        }
    } else {
        let mut join_set = tokio::task::JoinSet::new();
        for future in futures {
            join_set.spawn(future);
        }

        while let Some(res) = join_set.join_next().await {
            if let Err(e) = res {
                tracing::warn!("INTERNAL ERROR FOR MESSAGE{:?}: {:?}", &message, e);
                handle_message_delivery_failure(&mut message, payload.len());
            } else if let Ok(Err(e)) = res {
                tracing::warn!("Failed to deliver message {:?}. Reason: {:?}", &message, e.0);
                handle_message_delivery_failure(&mut message, payload.len());
            }
        }
    }

    message.total_sent_bytes_human_readable =
        bytesize::ByteSize::b(message.total_sent_bytes as u64).into();

    match message.delivery_failures {
        x if x == 0 => Ok(web::Json(message).customize()),
        x if x == message.message_number => Err(ResponseError::NoMessagesDelivered),
        _ => Ok(web::Json(message)
            .customize()
            .with_status(StatusCode::MULTI_STATUS)),
    }
}
