from uuid import UUID
from dataclasses import dataclass

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
    async: bool
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
