use actix_web::{post, web};
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

#[utoipa::path(
    tag = "messages",
    responses(
        (status = 200, description = "Sent a new message", body = SentMessage),
        (status = 404, description = "Experiment not found")
    )
)]
#[post("/")]
/// Send a new message
async fn send(
    params: web::Json<SendMessage>,
    data: web::Data<AppData>,
) -> actix_web::Result<web::Json<SentMessage>> {
    let params = params.into_inner();

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", params.brokers.clone())
        .set("queue.buffering.max.ms", params.buffering_ms.to_string())
        .set(
            "message.timeout.ms",
            params.message_timeout.0.as_millis().to_string(),
        );

    if params.ssl {
        config.set("security.protocol", "ssl");
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
            return Err(actix_web::error::ErrorNotFound("Experiment not found"));
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
                            .payload(&payload)
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

    let message = SentMessage {
        experiment_uuid: params.experiment_uuid,
        bytes_size: payload.len(),
        message_number: params.messages_number,
        total_sent_bytes: total_bytes,
        total_sent_bytes_human_readable: bytesize::ByteSize::b(total_bytes as u64).into(),
    };

    let mut join_set = tokio::task::JoinSet::new();
    for future in futures {
        join_set.spawn(future);
    }

    while let Some(res) = join_set.join_next().await {
        if let Err(e) = res {
            tracing::warn!("INTERNAL ERROR FOR MESSAGE{:?}: {:?}", &message, e);
        } else if let Ok(Err(e)) = res {
            tracing::warn!("Failed to deliver message {:?}. Reason: {:?}", &message, e);
        }
    }
    //for future in futures {

    //    if let Err(e) = future.await {
    //        tracing::warn!("Failed to deliver message {:?}. Reason: {:?}", &message, e);
    //    }
    //}

    Ok(web::Json(message))
}
