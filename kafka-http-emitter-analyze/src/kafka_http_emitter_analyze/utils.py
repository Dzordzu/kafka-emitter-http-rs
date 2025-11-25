from statistics import mean, median


def list_of_ints_stats(input: list[int]) -> dict[str, int | float]:
    return {
        "min": min(input),
        "max": max(input),
        "mean": mean(input),
        "median": median(input),
        "entries": len(input),
    }
