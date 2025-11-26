import kafka_http_emitter_analyze.typeadapters as typeadapters
from os import makedirs
from datetime import datetime
from kafka_http_emitter_analyze.utils import list_of_ints_stats, int_similar_percentage
from time import sleep
import threading
from time import sleep
from kafka_http_emitter_analyze.emitter_requests import (
    new_experiment,
    terminate_experiment,
    send_single_message_batch,
    send_job,
    get_bytes_size,
    get_kafka_latencies,
    get_send_receive_latencies,
)
from kafka_http_emitter_analyze.models import (
    MessageRequest,
    MessageRateRequest,
    JobRequest,
    ExperimentSummary,
    ExperimentSummaryPoint,
    CollectorCfg,
    ConfigFile,
)
from pydantic import FilePath
import pathlib
from logging import getLogger
import matplotlib.pyplot as plt

logger = getLogger(__name__)


def generate_graphs(
    name: str, filepath: str, experiment_summary: list[ExperimentSummaryPoint]
):

    initial_timestamp = experiment_summary[0].timestamp
    timestamps = [x.timestamp - initial_timestamp for x in experiment_summary]
    sent_messages_bytes_total = [
        x.experiment_summary.body_size.entries * x.experiment_summary.body_size.mean
        for x in experiment_summary
    ]
    sent_messages_bytes = [
        j - i
        for i, j in zip(sent_messages_bytes_total[:-1], sent_messages_bytes_total[1:])
    ]
    sent_messages_bytes.insert(0, 0)
    kafka_latencies_mean = [
        x.experiment_summary.kafka_latency.mean for x in experiment_summary
    ]
    kafka_latencies_median = [
        x.experiment_summary.kafka_latency.median for x in experiment_summary
    ]

    send_receive_latencies_mean = [
        x.experiment_summary.send_receive_latency.mean for x in experiment_summary
    ]
    send_receive_latencies_median = [
        x.experiment_summary.send_receive_latency.median for x in experiment_summary
    ]

    fig, axes = plt.subplots(3, 1, figsize=(10, 8), sharex=True)

    fig.suptitle(f"Summary for {name}")

    axes[0].plot(timestamps, sent_messages_bytes, marker="o")
    axes[0].set_title("Sent bytes")
    axes[0].set_ylabel("bytes")
    axes[0].set_xlabel("time (s)")
    axes[0].set_yscale("log", base=2)

    axes[1].plot(timestamps, kafka_latencies_mean, marker="o", label="mean")
    axes[1].plot(timestamps, kafka_latencies_median, marker="o", label="median")
    axes[1].set_title("Kafka latencies")
    axes[1].set_ylabel("latency (ms)")
    axes[1].set_xlabel("time (s)")

    axes[2].plot(timestamps, send_receive_latencies_mean, marker="o", label="mean")
    axes[2].plot(timestamps, send_receive_latencies_median, marker="o", label="median")
    axes[2].set_title("Send/receive latencies")
    axes[2].set_ylabel("latency (ms)")
    axes[2].set_xlabel("time (s)")

    plt.tight_layout()
    plt.legend(loc="best")

    plt.savefig(filepath)


def collect_data(collector_data: CollectorCfg, data: list[ExperimentSummaryPoint]):
    while True:
        sleep(collector_data.every_ms / 1000)

        try:
            body_sizes = get_bytes_size(
                collector_data.address, collector_data.experiment_uuid
            )
            kafka_latencies = get_kafka_latencies(
                collector_data.address,
                collector_data.experiment_uuid,
                collector_data.source,
                collector_data.dest,
            )
            send_receive_latency = get_send_receive_latencies(
                collector_data.address,
                collector_data.experiment_uuid,
                collector_data.source,
                collector_data.dest,
            )

            data.append(
                ExperimentSummaryPoint(
                    timestamp=datetime.now().timestamp(),
                    experiment_summary=ExperimentSummary(
                        body_size=list_of_ints_stats(body_sizes),
                        kafka_latency=list_of_ints_stats(kafka_latencies),
                        send_receive_latency=list_of_ints_stats(send_receive_latency),
                    ),
                )
            )

            if collector_data.should_end:
                return

            if int_similar_percentage(
                collector_data.expected_all_messages, len(send_receive_latency), 0.05
            ) or int_similar_percentage(
                collector_data.expected_all_messages, len(kafka_latencies), 0.05
            ):
                collector_data.should_end = True

        except Exception as e:
            logger.exception("Failed to get data", e)


def calculate_expected_messages(config: ConfigFile) -> int:
    result = 0
    for message in config.messages:
        match message.what:
            case "job":
                result += message.messages_number
            case "batch":
                result += message.messages_number
            case "wait":
                pass
    return result


def plotmode(file: FilePath):
    json_config = pathlib.Path(file).read_text()
    config = typeadapters.CONFIG_FILE.validate_json(json_config)

    experiment_uuid = new_experiment(
        address=config.address, source=config.source, dest=config.dest
    )

    sleep(config.wait_for_consumers_s)  # wait for consumers to join

    expected_all_messages = calculate_expected_messages(config)
    experiment_summary: list[ExperimentSummaryPoint] = []
    collector_data = CollectorCfg(
        address=config.address,
        every_ms=config.query_every_ms,
        experiment_uuid=experiment_uuid,
        source=config.source,
        dest=config.dest,
        should_end=False,
        expected_all_messages=expected_all_messages,
    )

    data_collector_thread = threading.Thread(
        target=collect_data, args=(collector_data, experiment_summary)
    )
    data_collector_thread.daemon = True
    data_collector_thread.start()

    for message in config.messages:
        match message.what:
            case "job":
                request = JobRequest(
                    body_size=message.body_size,
                    brokers=config.source.brokers,
                    buffering_ms=message.buffering_ms,
                    experiment_uuid=str(experiment_uuid),
                    message_rate=MessageRateRequest(
                        messages=message.message_rate.messages,
                        per=f"{message.message_rate.per_ms}ms",
                    ),
                    message_timeout=message.message_timeout,
                    messages_number=message.messages_number,
                    ssl=config.source.ssl,
                    topic=config.source.topic,
                )
                send_job(request, config.address)
            case "batch":
                request = MessageRequest(
                    async_mode=True,
                    blocking=False,
                    body_size=message.body_size,
                    brokers=config.source.brokers,
                    buffering_ms=message.buffering_ms,
                    experiment_uuid=str(experiment_uuid),
                    message_timeout=message.message_timeout,
                    messages_number=message.messages_number,
                    ssl=config.source.ssl,
                    topic=config.source.topic,
                )
                send_single_message_batch(request, config.address)
            case "wait":
                sleep(message.time_ms / 1000)

    collector_data.should_end = True
    sleep(config.query_every_ms / 1000)
    terminate_experiment(config.address, experiment_uuid)

    makedirs(pathlib.Path(config.output.image).parent, exist_ok=True)
    makedirs(pathlib.Path(config.output.summary).parent, exist_ok=True)

    summary: ExperimentSummaryPoint = experiment_summary[-1]
    with pathlib.Path(config.output.summary).open(mode="w") as f:
        f.write(
            typeadapters.EXPERIMENT_SUMMARY.dump_json(
                summary.experiment_summary, indent=2
            ).decode("UTF-8")
        )

    generate_graphs(config.experiment_name, config.output.image, experiment_summary)

    pass
