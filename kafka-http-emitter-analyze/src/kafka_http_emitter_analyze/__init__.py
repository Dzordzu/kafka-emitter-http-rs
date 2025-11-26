from pydantic import FilePath
from kafka_http_emitter_analyze.modes.simple import simple as _simple
from kafka_http_emitter_analyze.modes.plotmode import plotmode as _plotmode
import click


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
@click.option(
    "-M",
    "--buffering-ms",
    type=int,
    default=5,
    help=("How often should kafka buffer ms / how often should messagaes be sent"),
)
@click.option(
    "--blocking/--no-blocking",
    type=bool,
    default=False,
    help="How to send messages. Either one by one (blocking) or in a batch (no-blocking)",
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
    consumers_wait_for_s: float,
    blocking: bool,
    buffering_ms: int,
):
    _simple(
        address,
        source_brokers,
        dest_brokers,
        consumer_group,
        source_topic,
        dest_topic,
        message_timeout,
        use_ssl,
        number_of_messages,
        message_size,
        wait_for_s,
        consumers_wait_for_s,
        blocking,
        buffering_ms,
    )


@main.command(help="Run a test using a spec file")
@click.option(
    "-f", "--file", help="Path to the configuration", required=True, type=FilePath
)
def file(file: FilePath):
    _plotmode(file)
    pass
