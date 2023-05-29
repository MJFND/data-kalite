from dataclasses import dataclass, field
from functools import wraps
from typing import Any, Dict, List, Union

from great_expectations.dataset import SparkDFDataset
from pyspark.sql import DataFrame

from data.data_classes.metrics_data import MetricsData
from data.functions.base import Base
from data.utilities.logger import logger


class Metrics(Base):
    def __init__(
        self,
        source_data: DataFrame,
        config: Any = None,
        metadata: Dict = None,
        # TODO needs to find a better way to automatically calculate threshold
        unique_count_threshold: int = 25,
    ):
        """
        Initialize
        :param source_data: A Dataframe holding the actual source data
        :param config: Any dict config to derive metrics
        :param metadata: Pipeline metadata parameters, e.g. run id extra args etc.
        :param unique_count_threshold: Threshold used to calculate default metrics.
                             Defaults to 25, columns that have unique_count less than or equal to 25 will
                             calculate value level metrics like get_rate
        """
        super(Metrics, self).__init__(
            source_data=source_data,
            config=config,
            metadata=metadata,
            unique_count_threshold=unique_count_threshold,
        )

    @staticmethod
    def standardize(func: Any):
        """
        Decorator with wraps metrics for logging and converting to a DataFrame rows
        :param func:
        :return: self (GeValidation Object)
        """

        @wraps(func)
        def inner(self, *args, **kwargs):
            obj = func(self, *args, **kwargs)
            results = float(obj.results)
            column = obj.meta.get("column")
            column_value = obj.meta.get("column_value")
            logger.info(
                f"Metrics Result: Metrics {func.__name__} is {results}.\n"
                f"Returned output: {obj.__dict__}\n"
            )
            self._check.append(
                MetricsData(
                    column_name=column,
                    metrics=func.__name__.strip("_"),
                    column_value=column_value,
                    results=results,
                )
            )
            return self

        return inner

    def run(self) -> DataFrame:
        """
        Runs the metrics with schema handling and dataframe conversion

        :return: A Dataframe
        """
        return self.add_metadata_cols(
            self.to_df(
                data=self.metrics(),
                schema=self.get_spark_schema(MetricsData),
            ).distinct()
        )


@dataclass
class MetricsSuiteResult:
    """
    Similar to GE Validation Suite (ExpectationSuiteValidationResult),
    custom data class to wrap results in standard json
    """

    results: Union[str, int, float, List]
    meta: dict = field(default_factory=dict)


class GeMetrics:
    def __init__(self, data: DataFrame):
        """
        Initialize the data as GE SparkDFDataset which persist data under the hood!
        :param data:
        """
        self.ge_spark_df = SparkDFDataset(data)
        self._check = []

    def result(self) -> List[MetricsData]:
        """
        Returns the already list of metrics data
        :return:
        """
        return self._check

    def find_last_calculated_metric_index(self) -> int:
        """
        Finds the index of the last calculated metric that can be used to find values for future metrics
        :return:
        """
        return len(self._check)

    @Metrics.standardize
    def get_row_count(self):
        """
        Generates row count
        :return: Returns a self from the decorator
        """
        return MetricsSuiteResult(results=self.ge_spark_df.get_row_count())

    @Metrics.standardize
    def get_column_unique_count(self, column: str):
        """
        Generates column unique count
        :param column: Representing a column name on which we want to perform
        :return: Returns a self from the decorator
        """
        return MetricsSuiteResult(
            results=self.ge_spark_df.get_column_unique_count(column),
            meta={"column": column},
        )

    @Metrics.standardize
    def get_column_max(self, column: str):
        """
        Generates column maximum
        :param column: Representing a column name on which we want to perform
        :return: Returns a self from the decorator
        """
        return MetricsSuiteResult(
            results=self.ge_spark_df.get_column_max(column), meta={"column": column}
        )

    @Metrics.standardize
    def get_column_min(self, column: str):
        """
        Generates column minimum
        :param column: Representing a column name on which we want to perform
        :return: Returns a self from the decorator
        """
        return MetricsSuiteResult(
            results=self.ge_spark_df.get_column_min(column), meta={"column": column}
        )

    @Metrics.standardize
    def get_column_median(self, column: str):
        """
        Generates column median
        :param column: Representing a column name on which we want to perform
        :return: Returns a self from the decorator
        """
        return MetricsSuiteResult(
            results=self.ge_spark_df.get_column_median(column), meta={"column": column}
        )

    @Metrics.standardize
    def get_column_stdev(self, column: str):
        """
        Generates column standard deviation
        :param column: Representing a column name on which we want to perform
        :return: Returns a self from the decorator
        """
        return MetricsSuiteResult(
            results=self.ge_spark_df.get_column_stdev(column), meta={"column": column}
        )

    @Metrics.standardize
    def get_column_count_in_range(
        self, column: str, min_val: int = None, max_val: int = None
    ):
        """
        Generates column count in the given range
        :param column: Representing a column name on which we want to perform
        :param min_val:
        :param max_val:
        :return: Returns a self from the decorator
        """
        return MetricsSuiteResult(
            results=self.ge_spark_df.get_column_count_in_range(
                column, min_val, max_val
            ),
            meta={"column": column},
        )

    """
        Custom Metrics Start
    """

    @Metrics.standardize
    def _get_rate(self, column: str, rate: float, column_value: str):
        """
        Child function of get_rates with the goal to standardize results
        :param column: Representing a column name on which we want to perform
        :param rate: Resultant rate value
        :param column_value: Column Value on which rate is calculated
        :return: Returns a self from the decorator
        """
        return MetricsSuiteResult(
            results=rate, meta={"column": column, "column_value": column_value}
        )

    def get_rates(self, column: str, total_count: int):
        """
        Parent of _get_rate, iterates on each column value of the given column
        and calculates the rate.
        :param column: Representing a column name on which we want to perform
        :param total_count: Total Row count used in calculation
        :return: Returns a self from the decorator
        """
        series = self.ge_spark_df.get_column_value_counts(column)
        for column_value, count in series.items():
            rate = float(count) / float(total_count)
            self._get_rate(column, rate, column_value)

        return self

    @Metrics.standardize
    def _get_column_value_count(self, column: str, count: int, column_value: str):
        """
        Child function of get_column_values_count with the goal to standardize results
        :param column: Representing a column name on which we want to perform
        :param count: Resultant count value
        :param column_value: Column Value on which count is calculated
        :return: Returns a self from the decorator
        """
        return MetricsSuiteResult(
            results=count, meta={"column": column, "column_value": column_value}
        )

    def get_column_values_count(self, column: str):
        """
        Parent of get_column_value_count, iterates on each column value of the
        given column and calculate the count.
        :param column: Representing a column name on which we want to perform
        :return: Returns a self from the decorator
        """
        series = self.ge_spark_df.get_column_value_counts(column)
        for column_value, count in series.items():
            self._get_column_value_count(column, count, column_value)

        return self
