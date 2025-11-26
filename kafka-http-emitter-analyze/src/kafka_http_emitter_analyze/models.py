from uuid import UUID
from pydantic.dataclasses import dataclass
from typing import Literal, Annotated
from pydantic import Field, Tag


@dataclass
class BrokerCfg:
    brokers: str
    consumer_group_id: str
    message_timeout: str
    ssl: bool
    topic: str


@dataclass
class ExperimentData:
    experiment_uuid: UUID
    source: BrokerCfg
    dest: BrokerCfg


@dataclass
class MessageRequest:
    async_mode: bool
    blocking: bool
    body_size: str
    brokers: str
    buffering_ms: int
    experiment_uuid: str
    message_timeout: str
    messages_number: int
    ssl: bool
    topic: str


@dataclass
class FilteringRequest:
    brokers: str
    consumer_group: str
    topic: str


@dataclass
class MessageRate:
    messages: int
    per_ms: int


@dataclass
class MessageRateRequest:
    messages: int
    per: str

@dataclass
class ConfigWait:
    what: Literal["wait"]
    time_ms: int


@dataclass
class ConfigJob:
    what: Literal["job"]
    body_size: str
    message_rate: MessageRate
    messages_number: int
    message_timeout: str = Field(default="10s")
    buffering_ms: int = Field(default=5)


@dataclass
class ConfigBatch:
    what: Literal["batch"]
    body_size: str
    messages_number: int
    message_timeout: str = Field(default="10s")
    buffering_ms: int = Field(default=5)


@dataclass
class ConfigOutput:
    image: str
    summary: str


@dataclass
class ConfigFile:
    experiment_name: str
    address: str
    source: BrokerCfg
    dest: BrokerCfg
    query_every_ms: int
    output: ConfigOutput
    messages: list[
        Annotated[ConfigJob | ConfigWait | ConfigBatch, Field(discriminator="what")]
    ]
    wait_for_consumers_s: float | int = Field(default = 5)

@dataclass
class JobRequest:
    body_size: str
    brokers: str
    buffering_ms: int
    experiment_uuid: str
    message_rate: MessageRateRequest
    message_timeout: str
    messages_number: int
    ssl: bool
    topic: str

@dataclass
class IntListSummary:
    min: float
    max: float
    mean: float
    median: float
    entries: int

@dataclass
class ExperimentSummary:
    body_size: IntListSummary
    kafka_latency: IntListSummary
    send_receive_latency: IntListSummary

@dataclass
class ExperimentSummaryPoint:
    timestamp: float
    experiment_summary: ExperimentSummary

@dataclass
class CollectorCfg:
    """ Configuration for collector thread in plotmode """
    address: str
    experiment_uuid: UUID
    source: BrokerCfg
    dest: BrokerCfg
    should_end: bool
    every_ms: int
    expected_all_messages: int
