use std::collections::HashMap;

use crate::models::{EventType, measurements::*};
use actix_web::{post, web};

use crate::AppData;

#[utoipa::path(
    tag = "measurements",
    responses(
        (status = 200, description = "Experiment stats about average message payload size in bytes", body = Vec<u128>),
        (status = 404, description = "Experiment not found"),
    )
)]
/// Get messages sizes
#[post("/bytes-size")]
async fn messaged_bytes_size(
    params: web::Json<BytesSizeRequest>,
    data: web::Data<AppData>,
) -> actix_web::Result<web::Json<Vec<u128>>> {
    let (_, messages, _) = {
        data.experiment_related_data(&params.0.experiment_uuid)
            .await
            .ok_or(actix_web::error::ErrorNotFound("Experiment not found"))
    }?;

    Ok(web::Json(
        messages
            .0
            .into_values()
            .map(|mess| mess.bytes_size.as_bytes())
            .collect(),
    ))
}

#[utoipa::path(
    tag = "measurements",
    responses(
        (status = 200, description = "Latencies calculated from send/receive events", body = Stats),
        (status = 404, description = "Experiment not found"),
    )
)]
#[post("/send-receive-latency")]
/// Get statistical information about latencies calculated from
/// send/receive reported from different consumers
async fn send_receive_latencies(
    params: web::Json<SendReceiveLatencyRequest>,
    data: web::Data<AppData>,
) -> actix_web::Result<web::Json<Vec<u128>>> {
    let (_, _, events) = {
        data.experiment_related_data(&params.0.experiment_uuid)
            .await
            .ok_or(actix_web::error::ErrorNotFound("Experiment not found"))
    }?;

    let kafka_sent_events_source = events
        .iter()
        .filter(|event| matches!(&event.event_type, EventType::Sent))
        .filter(|event| {
            event.topic == params.source.topic && event.brokers == params.source.brokers
        });

    let kafka_received_events_dest = events
        .iter()
        .filter(|event| {
            matches! {&event.event_type, EventType::Received { consumer_group }
            if consumer_group == &params.0.dest.consumer_group}
        })
        .filter(|event| event.topic == params.dest.topic && event.brokers == params.dest.brokers);

    let mut source_timestamps = HashMap::new();

    for source in kafka_sent_events_source {
        source_timestamps.insert(source.message_uuid, source.timestamp_millis);
    }

    let mut result = Vec::new();

    for dest in kafka_received_events_dest {
        if let Some(source_value) = source_timestamps.get(&dest.message_uuid).cloned() {
            if dest.timestamp_millis >= source_value {
                result.push(dest.timestamp_millis - source_value)
            } else {
                tracing::warn!("Destination timestamp is lower than source timestamp");
            }
        }
    }

    Ok(web::Json(result))
}
#[utoipa::path(
    tag = "measurements",
    responses(
        (status = 200, description = "Get summarized experiment data", body = Vec<u128>),
        (status = 404, description = "Experiment not found"),
    )
)]
#[post("/kafka-latencies")]
/// Get statistical information about latencies reported by kafka from different consumers
async fn kafka_latencies(
    params: web::Json<KafkaLatencyRequest>,
    data: web::Data<AppData>,
) -> actix_web::Result<web::Json<Vec<u128>>> {
    let (_, _, events) = {
        data.experiment_related_data(&params.0.experiment_uuid)
            .await
            .ok_or(actix_web::error::ErrorNotFound("Experiment not found"))
    }?;

    let kafka_received_events_source = events
        .iter()
        .filter(|event| {
            matches! {&event.event_type, EventType::Received { consumer_group }
            if consumer_group == &params.0.source.consumer_group}
        })
        .filter(|event| {
            event.topic == params.source.topic && event.brokers == params.source.brokers
        });

    let kafka_received_events_dest = events
        .iter()
        .filter(|event| {
            matches! {&event.event_type, EventType::Received { consumer_group }
            if consumer_group == &params.0.dest.consumer_group}
        })
        .filter(|event| event.topic == params.dest.topic && event.brokers == params.dest.brokers);

    let mut source_timestamps = HashMap::new();

    for source in kafka_received_events_source {
        source_timestamps.insert(source.message_uuid, source.timestamp_millis);
    }

    let mut result = Vec::new();

    for dest in kafka_received_events_dest {
        if let Some(source_value) = source_timestamps.get(&dest.message_uuid).cloned() {
            if dest.timestamp_millis >= source_value {
                result.push(dest.timestamp_millis - source_value)
            } else {
                tracing::warn!("Destination timestamp is lower than source timestamp");
            }
        }
    }

    Ok(web::Json(result))
}
