use crate::models::KafkaBrokerCfg;
use crate::models::{EventType, Message, MessageEvent};
use crate::state::MessagesState;
use crate::{EXPERIMENT_UUID_HEADER, MESSAGE_UUID_HEADER, get_now_millis};
use rdkafka::consumer::Consumer as _;
use rdkafka::message::{Headers, Message as _};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::warn;
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct LoopHandler {
    handles: HashMap<Uuid, Vec<tokio::task::AbortHandle>>,
}

impl LoopHandler {
    pub fn new() -> Self {
        Self {
            handles: Default::default(),
        }
    }

    pub fn register_experiment(
        &mut self,
        uuid: Uuid,
        handles: Vec<tokio::task::AbortHandle>,
    ) -> &mut Self {
        self.handles.insert(uuid, handles);
        self
    }

    pub fn deregister_experiment(&mut self, uuid: &Uuid) -> &mut Self {
        if let Some(handles) = self.handles.remove(uuid) {
            for handle in handles {
                handle.abort();
            }
        }

        self
    }
}

#[derive(Debug, Clone)]
pub struct Consumers {
    loops: LoopHandler,
    state_container: Arc<Mutex<MessagesState>>,
}

impl Consumers {
    pub fn new(events_container: Arc<Mutex<MessagesState>>) -> Self {
        Self {
            loops: Default::default(),
            state_container: events_container,
        }
    }

    pub async fn start(
        &mut self,
        uuid: Uuid,
        cfg: KafkaBrokerCfg,
        offset_reset: bool,
    ) -> &mut Self {
        let events = self.state_container.clone();
        let handle = tokio::spawn(async move { consumer_loop(cfg, events, offset_reset).await })
            .abort_handle();

        let vec_of_handles = self.loops.handles.entry(uuid).or_default();
        vec_of_handles.push(handle);

        self
    }

    pub fn stop(&mut self, uuid: &Uuid) -> &mut Self {
        self.loops.deregister_experiment(uuid);
        self
    }
}

pub async fn consumer_loop(
    cfg: KafkaBrokerCfg,
    state: Arc<Mutex<MessagesState>>,
    offset_reset: bool,
) {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::DefaultConsumerContext;
    use rdkafka::consumer::stream_consumer::StreamConsumer;

    let mut config = ClientConfig::new();

    config
        .set("group.id", cfg.consumer_group_id.clone())
        .set("bootstrap.servers", cfg.brokers.clone())
        .set("enable.auto.commit", "true")
        .set(
            "max.poll.interval.ms",
            (cfg.message_timeout.0.as_millis() * 2).to_string(),
        )
        .set(
            "session.timeout.ms",
            cfg.message_timeout.0.as_millis().to_string(),
        )
        .set("enable.auto.commit", "false");

    if cfg.ssl {
        config.set("security.protocol", "ssl");
        config.set("enable.ssl.certificate.verification", "false");
    }

    if offset_reset {
        config.set("auto.offset.reset", "earliest");
        tracing::info!("Consuming messages from beginning");
    }

    let consumer: StreamConsumer<DefaultConsumerContext> =
        config.create().expect("Consumer creation failed");

    consumer
        .subscribe(&[&cfg.topic])
        .expect("Could not subscribe fucker");

    loop {
        match consumer.recv().await {
            Err(e) => warn!("Kafka error: {}", e),
            Ok(m) => {
                let now = get_now_millis();
                let headers: HashMap<String, String> = if let Some(headers) = m.headers() {
                    headers
                        .iter()
                        .filter_map(|header| {
                            let value = header.value.map(std::str::from_utf8).and_then(|x| x.ok());

                            value.map(|x| (header.key.to_string(), x.to_string()))
                        })
                        .collect()
                } else {
                    HashMap::new()
                };

                if let Some(message_uuid_str) = headers.get(MESSAGE_UUID_HEADER)
                    && let Ok(message_uuid) = uuid::Uuid::try_parse(message_uuid_str)
                    && let Some(experiment_uuid_str) = headers.get(EXPERIMENT_UUID_HEADER)
                    && let Ok(experiment_uuid) = uuid::Uuid::try_parse(experiment_uuid_str)
                {
                    let mut state = state.lock().await;

                    if (!state.messages.contains_key(&message_uuid))
                        && state.experiments.contains_key(&experiment_uuid)
                    {
                        state.insert_message(
                            Message {
                                uuid: message_uuid,
                                bytes_size: bytesize::ByteSize::b(m.payload_len() as u64).into(),
                            },
                            experiment_uuid,
                            false,
                        );
                    }

                    if state
                        .messages
                        .get(&experiment_uuid)
                        .is_some_and(|experiment_messages| {
                            experiment_messages.0.contains_key(&message_uuid)
                        })
                    {
                        let experiment_uuid = {
                            *state
                                .idx_message_to_experiment
                                .0
                                .get(&message_uuid)
                                .unwrap()
                        };

                        let events = state.events.entry(experiment_uuid).or_default();

                        events.push(MessageEvent {
                            message_uuid,
                            timestamp_millis: now,
                            topic: m.topic().into(),
                            brokers: cfg.brokers.clone(),
                            event_type: EventType::Received {
                                consumer_group: cfg.consumer_group_id.clone(),
                            },
                        });

                        if let Some(millis) = m.timestamp().to_millis() {
                            events.push(MessageEvent {
                                message_uuid,
                                timestamp_millis: millis as u128,
                                topic: m.topic().into(),
                                brokers: cfg.brokers.clone(),
                                event_type: EventType::KafkaTimestampSet {
                                    consumer_group: cfg.consumer_group_id.clone(),
                                },
                            });
                        }

                        tracing::log::debug!("Message {} consumed", message_uuid);
                    } else {
                        tracing::log::debug!("Message is not destined for us. Skipping");
                    }
                }
            }
        }
    }
}
