import kafka_http_emitter_analyze.typeadapters as typeadapters
from logging import getLogger
import requests
from uuid import UUID
from kafka_http_emitter_analyze.consts import *
from kafka_http_emitter_analyze.models import (
    JobRequest,
    BrokerCfg,
    MessageRequest,
    FilteringRequest,
)

logger = getLogger(__name__)


def new_experiment(address: str, source: BrokerCfg, dest: BrokerCfg) -> UUID:

    new_experiment_body = {
        "listeners": [
            typeadapters.BROKER_CFG.dump_python(source),
            typeadapters.BROKER_CFG.dump_python(dest),
        ]
    }

    response = requests.post(f"{address}/experiment/", json=new_experiment_body)
    response.raise_for_status()
    uuid_str = response.json()[EXPERIMENT_UUID_KEY]
    return UUID(uuid_str)


def terminate_experiment(address: str, experiment_uuid: UUID):
    response = requests.delete(
        f"{address}/experiment/", json={EXPERIMENT_UUID_KEY: str(experiment_uuid)}
    )
    response.raise_for_status()


def send_single_message_batch(request: MessageRequest, address: str):
    body = typeadapters.MESSAGE_REQUESTS.dump_python(request)
    response = requests.post(f"{address}/message/", json=body)
    if response.status_code != 200:
        logger.error("Failure for message body: %s", body)
        logger.error(response.text)
    response.raise_for_status()


def send_job(request: JobRequest, address: str):
    body = typeadapters.JOB_REQUEST.dump_python(request)
    response = requests.post(f"{address}/message/job", json=body)
    if response.status_code != 200:
        logger.error("Failure for message body: %s", body)
        logger.error(response.text)
    response.raise_for_status()


def simple_emit_messages(
    address: str,
    experiment_uuid: UUID,
    source: BrokerCfg,
    messages_numer: int,
    message_size: str,
    blocking: bool,
    buffering_ms: int,
):
    request = MessageRequest(
        async_mode=True,
        blocking=blocking,
        body_size=message_size,
        brokers=source.brokers,
        buffering_ms=buffering_ms,
        experiment_uuid=str(experiment_uuid),
        messages_number=messages_numer,
        ssl=source.ssl,
        topic=source.topic,
        message_timeout=source.message_timeout,
    )
    send_single_message_batch(request, address)


def get_bytes_size(address: str, experiment_uuid: UUID) -> list[int]:
    response = requests.post(
        f"{address}/measurements/bytes-size",
        json={EXPERIMENT_UUID_KEY: str(experiment_uuid)},
    )
    response.raise_for_status()

    return response.json()


def __get_latencies(
    address: str,
    endpoint: str,
    experiment_uuid: UUID,
    source: BrokerCfg,
    dest: BrokerCfg,
) -> list[int]:

    body = {
        EXPERIMENT_UUID_KEY: str(experiment_uuid),
        "source": typeadapters.FILTERING_REQUEST.dump_python(
            FilteringRequest(
                brokers=source.brokers,
                consumer_group=source.consumer_group_id,
                topic=source.topic,
            )
        ),
        "dest": typeadapters.FILTERING_REQUEST.dump_python(
            FilteringRequest(
                brokers=dest.brokers,
                consumer_group=dest.consumer_group_id,
                topic=dest.topic,
            )
        ),
    }

    response = requests.post(
        f"{address}/measurements/{endpoint}",
        json=body,
    )
    response.raise_for_status()

    return response.json()


def get_kafka_latencies(
    address: str,
    experiment_uuid: UUID,
    source: BrokerCfg,
    dest: BrokerCfg,
) -> list[int]:
    return __get_latencies(
        address=address,
        endpoint="kafka-latencies",
        experiment_uuid=experiment_uuid,
        source=source,
        dest=dest,
    )


def get_send_receive_latencies(
    address: str,
    experiment_uuid: UUID,
    source: BrokerCfg,
    dest: BrokerCfg,
) -> list[int]:
    return __get_latencies(
        address=address,
        endpoint="send-receive-latency",
        experiment_uuid=experiment_uuid,
        source=source,
        dest=dest,
    )
