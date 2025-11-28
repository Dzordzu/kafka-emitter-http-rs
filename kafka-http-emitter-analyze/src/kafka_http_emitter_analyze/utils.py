from kafka_http_emitter_analyze.models import IntListSummary, ExperimentSummary
from statistics import mean, median


def list_of_ints_stats(input: list[int]) -> IntListSummary:
    input = [x for x in input if x != 0]
    if not input:
        input = [0]

    return IntListSummary(
        min=min(input),
        max=max(input),
        mean=mean(input),
        median=median(input),
        entries=len(input),
    )


def default_list_of_ints_stats() -> IntListSummary:
    return IntListSummary(
        min=0,
        max=0,
        mean=0,
        median=0,
        entries=0,
    )


def default_experiment_summary() -> ExperimentSummary:
    return ExperimentSummary(
        body_size=default_list_of_ints_stats(),
        kafka_latency=default_list_of_ints_stats(),
        send_receive_latency=default_list_of_ints_stats(),
    )

def int_similar_percentage(a: int, b: int, pct: float) -> bool:
    """ 
    Example:
    int_similar_percentage(100, 105, pct=0.10)   # True (10% margin)
    """
    diff = abs(a - b)
    return diff <= max(a, b) * pct
