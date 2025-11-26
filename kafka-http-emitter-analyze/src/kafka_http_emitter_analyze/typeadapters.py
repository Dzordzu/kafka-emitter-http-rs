from pydantic import TypeAdapter
from kafka_http_emitter_analyze.models import (
    MessageRequest,
    JobRequest,
    BrokerCfg,
    FilteringRequest,
    ExperimentSummary,
    ConfigFile,
)

MESSAGE_REQUESTS = TypeAdapter(MessageRequest)
JOB_REQUEST = TypeAdapter(JobRequest)
BROKER_CFG = TypeAdapter(BrokerCfg)
FILTERING_REQUEST = TypeAdapter(FilteringRequest)
EXPERIMENT_SUMMARY = TypeAdapter(ExperimentSummary)
CONFIG_FILE = TypeAdapter(ConfigFile)
