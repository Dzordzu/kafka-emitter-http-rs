use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToResponse, ToSchema};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, IntoParams)]
pub struct SendReceiveLatencyRequestBrokerSource {
    #[schema(examples(crate::models::default_brokers))]
    pub brokers: String,

    #[schema(examples(crate::models::default_topic))]
    pub topic: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, IntoParams)]
pub struct KafkaLatencyRequestBroker {
    #[schema(examples(crate::models::default_brokers))]
    pub brokers: String,

    #[schema(examples(crate::models::default_topic))]
    pub topic: String,

    #[schema(examples("default-consumer-group"))]
    pub consumer_group: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, IntoParams)]
pub struct KafkaLatencyRequest {
    pub experiment_uuid: Uuid,
    pub source: KafkaLatencyRequestBroker,
    pub dest: KafkaLatencyRequestBroker,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, IntoParams)]
pub struct SendReceiveLatencyRequest {
    pub experiment_uuid: Uuid,
    pub source: SendReceiveLatencyRequestBrokerSource,
    pub dest: KafkaLatencyRequestBroker,
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, IntoParams)]
pub struct BytesSizeRequest {
    pub experiment_uuid: Uuid,
}

#[derive(Serialize, Deserialize, ToSchema, ToResponse, Debug, Clone)]
pub struct StatsLatencies {
    /// Difference between times in source and dest kafka timestamps
    pub kafka_latencies_ms: MinMaxAvg,

    /// Difference in times of producing (source kafka) and consuming (dest kafka)
    pub latencies_ms: MinMaxAvg,
}

#[derive(Serialize, Deserialize, ToSchema, ToResponse, Debug, Clone)]
pub struct MinMaxAvg {
    pub min: u128,
    pub max: u128,
    pub avg: u128,
}

#[derive(Serialize, Deserialize, ToSchema, ToResponse, Debug, Clone)]
pub struct TotalAvg {
    pub total: u128,
    pub avg: u128,
}

#[derive(Serialize, Deserialize, ToSchema, ToResponse, Debug, Clone)]
pub struct BytesSizeStats {
    /// Bytes stats related to the events (consumed, received, received by kafka)
    pub messages: TotalAvg,

    /// Bytes stats related to the received messages
    pub received_messages: TotalAvg,

    /// Bytes stats related to the sent messages
    pub sent_messages: TotalAvg,
}

#[derive(Serialize, Deserialize, ToSchema, ToResponse, Debug, Clone)]
pub struct Stats {
    /// Number of recorded events (e.g. message sent, received, kafka message timestamp)
    pub recorded_events_number: usize,

    /// Timestamp of the experiment creation date
    pub experiment_start_timestamp_ms: u128,

    /// Information about kafka message size
    pub bytes_size: BytesSizeStats,

    /// Information about latencies
    pub latencies: StatsLatencies,
}
