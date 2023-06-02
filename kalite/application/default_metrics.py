from typing import List

from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    NumericType,
    StringType,
)

from kalite.functions.metrics import GeMetrics, Metrics, MetricsData


class DefaultMetrics(Metrics):
    def metrics(self) -> List[MetricsData]:
        """
        Default Metrics generator that takes in a DataFrame and
        produces metrics against all columns

        :return: List of Metrics Object
        """
        ge_metrics = GeMetrics(self.source_data)
        start_index = ge_metrics.find_last_calculated_metric_index()
        ge_metrics = ge_metrics.get_row_count()
        row_count = ge_metrics.result()[start_index].results

        for column in self.source_data.schema.fields:
            # If its a numerical then run min, max, median
            if isinstance(
                column.dataType,
                (IntegerType, DecimalType, DoubleType, FloatType, NumericType),
            ):
                ge_metrics.get_column_max(column.name).get_column_min(
                    column.name
                ).get_column_median(column.name)
            else:
                start_index = ge_metrics.find_last_calculated_metric_index()
                ge_metrics = ge_metrics.get_column_unique_count(column.name)
                unique_count = ge_metrics.result()[start_index].results
                if isinstance(column.dataType, StringType):
                    # Following checks cannot be run on Date and TimeStamp Columns
                    # Checks set col `column_value` which requires to be STRING
                    # Check: README.md#Limitations
                    if unique_count <= self.unique_count_threshold:
                        ge_metrics.get_column_values_count(column.name).get_rates(
                            column.name, int(row_count)
                        )
        return ge_metrics.result()
