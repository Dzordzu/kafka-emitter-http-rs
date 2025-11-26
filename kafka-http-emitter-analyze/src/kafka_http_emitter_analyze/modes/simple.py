import kafka_http_emitter_analyze.typeadapters as typeadapters
from kafka_http_emitter_analyze.utils import list_of_ints_stats
import json
from time import sleep
from kafka_http_emitter_analyze.models import BrokerCfg, ExperimentSummary
from logging import getLogger
from kafka_http_emitter_analyze.emitter_requests import (
    new_experiment,
    terminate_experiment,
    simple_emit_messages,
    get_bytes_size,
    get_kafka_latencies,
    get_send_receive_latencies,
)

logger = getLogger(__name__)

def simple(
    address: str,
    source_brokers: str,
    dest_brokers: str,
    consumer_group: str,
    source_topic: str,
    dest_topic: str,
    message_timeout: str,
    use_ssl: bool,
    number_of_messages: int,
    message_size: str,
    wait_for_s: int,
    consumers_wait_for_s: float,
    blocking: bool,
    buffering_ms: int,
):

    source_kafka = BrokerCfg(
        brokers=source_brokers,
        consumer_group_id=consumer_group,
        message_timeout=message_timeout,
        ssl=use_ssl,
        topic=source_topic,
    )

    dest_kafka = BrokerCfg(
        brokers=dest_brokers,
        consumer_group_id=consumer_group,
        message_timeout=message_timeout,
        ssl=use_ssl,
        topic=dest_topic,
    )

    experiment_uuid = new_experiment(
        address=address,
        source=source_kafka,
        dest=dest_kafka,
    )

    sleep(consumers_wait_for_s)  # wait for consumers to join

    try:
        simple_emit_messages(
            address=address,
            experiment_uuid=experiment_uuid,
            source=source_kafka,
            messages_numer=number_of_messages,
            message_size=message_size,
            blocking=blocking,
            buffering_ms=buffering_ms,
        )

        sleep(wait_for_s)

        body_sizes = get_bytes_size(address, experiment_uuid)
        kafka_latencies = get_kafka_latencies(
            address, experiment_uuid, source_kafka, dest_kafka
        )
        send_receive_latency = get_send_receive_latencies(
            address, experiment_uuid, source_kafka, dest_kafka
        )

        print(
            typeadapters.EXPERIMENT_SUMMARY.dump_json(
                ExperimentSummary(
                    body_size=list_of_ints_stats(body_sizes),
                    kafka_latency=list_of_ints_stats(kafka_latencies),
                    send_receive_latency=list_of_ints_stats(send_receive_latency),
                )
            )
        )

    except Exception as e:
        logger.error(e)

    terminate_experiment(address=address, experiment_uuid=experiment_uuid)
