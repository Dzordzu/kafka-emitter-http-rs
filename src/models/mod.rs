pub mod measurements;

use serde::{Deserialize, Serialize};
use utoipa::{
    IntoParams, PartialSchema, ToResponse, ToSchema,
    openapi::{ObjectBuilder, RefOr, Schema, schema::SchemaType},
};
use uuid::Uuid;

use crate::get_now_millis;

pub fn default_brokers() -> String {
    std::env::var(crate::DEFAULT_BROKERS_ENV).unwrap_or("localhost:9092".into())
}

pub fn default_topic() -> String {
    std::env::var(crate::DEFAULT_TOPIC_ENV).unwrap_or("default".into())
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ByteSize(bytesize::ByteSize);

impl ByteSize {
    pub fn as_bytes(&self) -> u128 {
        self.0.0 as u128
    }
}

impl From<bytesize::ByteSize> for ByteSize {
    fn from(value: bytesize::ByteSize) -> Self {
        Self(value)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Duration(#[serde(with = "humantime_serde")] pub std::time::Duration);

impl PartialSchema for ByteSize {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(utoipa::openapi::Type::String))
                .description(Some(
                    "bytes representation using IEC 60027-2 and ISO 80000-1",
                ))
                .examples(vec!["10KiB"])
                .build(),
        ))
    }
}

impl PartialSchema for Duration {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .schema_type(SchemaType::Type(utoipa::openapi::Type::String))
                .description(Some(
                    "time representation using free form like 15days 2min 2s",
                ))
                .examples(vec!["10s", "100h"])
                .build(),
        ))
    }
}

impl ToSchema for ByteSize {}
impl ToSchema for Duration {}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct KafkaBrokerCfg {
    #[schema(examples(default_brokers))]
    pub brokers: String,

    #[schema(examples(default_topic))]
    pub topic: String,

    #[schema(examples(false))]
    pub ssl: bool,
    pub message_timeout: Duration,

    #[schema(examples("default-consumer-group"))]
    pub consumer_group_id: String,
}

fn default_body_size() -> ByteSize {
    ByteSize(bytesize::ByteSize::kib(6))
}

fn default_messages_number() -> u32 {
    1
}

fn default_timeout() -> Duration {
    Duration(std::time::Duration::from_secs(60))
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct NewExperiment {
    pub listeners: Vec<KafkaBrokerCfg>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct RestoreExperiment {
    pub experiment_uuid: Uuid,
    pub listeners: Vec<KafkaBrokerCfg>,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct NewSimple {
    pub source: KafkaBrokerCfg,
    pub dest: KafkaBrokerCfg,

    #[schema(examples("default-consumer-group"))]
    pub consumer_group_id: String,

    #[serde(default = "default_body_size")]
    message_body_size: ByteSize,

    #[serde(default = "default_messages_number")]
    #[schema(examples(default_messages_number))]
    messages_number: u32,

    #[serde(default = "default_timeout")]
    timeout: Duration,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToResponse, ToSchema)]
pub struct BeginResponse {
    pub experiment_uuid: Uuid,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct EndRequest {
    pub experiment_uuid: Uuid,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToResponse, ToSchema)]
pub struct EndResponse {
    pub experiment_uuid: Uuid,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
pub enum EventType {
    Sent,
    KafkaTimestampSet { consumer_group: String },
    Received { consumer_group: String },
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
pub struct MessageEvent {
    pub message_uuid: Uuid,
    pub timestamp_millis: u128,
    pub brokers: String,
    pub topic: String,
    pub event_type: EventType,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
pub struct Experiment {
    pub uuid: Uuid,
    pub consumers: Vec<KafkaBrokerCfg>,
    pub experiment_start_timestamp_millis: u128,
    pub experiment_end_timestamp_millis: Option<u128>,
}

impl Experiment {
    pub fn new(uuid: Uuid, consumers: Vec<KafkaBrokerCfg>) -> Self {
        Self {
            uuid,
            consumers,
            experiment_start_timestamp_millis: get_now_millis(),
            experiment_end_timestamp_millis: None,
        }
    }
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
pub struct Message {
    pub uuid: Uuid,
    pub bytes_size: ByteSize,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
pub struct SentMessage {
    pub experiment_uuid: Uuid,
    pub message_number: usize,
    /// Bytes of the each sent message(s) in bytes
    pub bytes_size: usize,
    /// Sum of the sent bytes
    pub total_sent_bytes: usize,
    pub total_sent_bytes_human_readable: ByteSize,
    pub delivery_failures: usize,
}

#[derive(Serialize, Deserialize, ToSchema, Debug, Clone)]
pub struct Insights {
    pub messages: Vec<Message>,
    pub experiment: Experiment,
    pub events: Vec<MessageEvent>,
}

pub fn default_buffering_ms() -> u32 {
    5
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct SendMessage {
    #[schema(examples(5))]
    #[serde(default = "default_buffering_ms")]
    pub buffering_ms: u32,

    #[schema(examples(default_brokers))]
    pub brokers: String,

    #[schema(examples(default_topic))]
    pub topic: String,

    #[schema(examples(false))]
    pub ssl: bool,

    pub message_timeout: Duration,
    pub body_size: ByteSize,

    #[schema(examples(1))]
    pub messages_number: usize,
    pub experiment_uuid: Uuid,

    #[serde(default)]
    #[schema(examples(false))]
    pub blocking: bool
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, IntoParams)]
pub struct InsightsRequest {
    pub experiment_uuid: Uuid,
}
