from kafka_http_emitter_analyze.utils import list_of_ints_stats
import json
from time import sleep
from kafka_http_emitter_analyze.models import BrokerCfg
import click
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


@click.group()
def main():
    pass


@main.command(help="Run a simple test")
@click.option("-a", "--address", help="kafka-http-emitter-rs address", required=True)
@click.option(
    "-s",
    "--source-brokers",
    help="Address of the experiment source brokers",
    type=str,
    required=True,
)
@click.option(
    "-d",
    "--dest-brokers",
    help="Address of the experiment destination brokers",
    type=str,
    required=True,
)
@click.option(
    "-c",
    "--consumer-group",
    help="Name of the consumer group (group.id)",
    type=str,
    required=True,
)
@click.option("-n", "--number-of-messages", type=int, required=True)
@click.option(
    "-m",
    "--message-size",
    type=str,
    required=True,
    help="Message size in the text format (e.g. 10KiB)",
)
@click.option(
    "-t",
    "--source-topic",
    type=str,
    required=True,
    help="Name of the topic for the source kafka",
)
@click.option(
    "-T",
    "--dest-topic",
    type=str,
    required=True,
    help="Name of the topic for the source kafka",
)
@click.option(
    "-w",
    "--wait-for-s",
    type=int,
    default=10,
    help="Wait time in seconds after emitting before gathering results",
)
@click.option(
    "-W",
    "--consumers-wait-for-s",
    type=float,
    default=5,
    help=(
        "Wait time in seconds before emitting any messages."
        "We need consumers to start"
    ),
)
@click.option("--message-timeout", type=str, default="10s")
@click.option("--use-ssl/--no-ssl", type=bool, default=True)
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
    consumers_wait_for_s: float
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
            json.dumps(
                {
                    "body-size": list_of_ints_stats(body_sizes),
                    "kafka-latency": list_of_ints_stats(kafka_latencies),
                    "send-receive-latency": list_of_ints_stats(send_receive_latency),
                }
            )
        )

    except Exception as e:
        logger.error(e)

    terminate_experiment(address=address, experiment_uuid=experiment_uuid)
