from dataclasses import dataclass


@dataclass
class MetricsData:
    """
    Data Class which wraps the metric in standard Dataframe row.
    """

    metrics: str
    results: float
    column_name: str = None
    column_value: str = None
