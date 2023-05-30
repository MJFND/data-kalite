import os
from typing import Dict, List, Tuple

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from kalite.data_classes.metrics_data import MetricsData
from kalite.functions.metrics import GeMetrics, Metrics


class TestMetrics:
    """
    Each unit test runs one validation on same data
    Having duplicate lines is fine for unit test, DAMP > DRY for unit tests

    Tests are performed via the lower level api `run()` that returns a Dataframe
    """

    @pytest.fixture(scope="session")
    def setup(self, request):
        """
        Fixture for creating a spark context and data.
        Used across all tests cases.
        """

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("Test Metrics")
            .getOrCreate()
        )
        # Last step to stop spark before the session ends
        request.addfinalizer(lambda: spark.stop())

        # Load test data
        data_path = (
            os.path.dirname(os.path.realpath(__file__)) + "../../resources/mock.csv"
        )
        df_reader = spark.read.format("csv").options(
            **{"header": "true", "inferSchema": "true"}
        )
        source_data = df_reader.load(data_path)
        # pipeline args is not used in assertions
        metadata = {
            "dag_id": "mydag",
            "dag_task_id": "mytask",
            "dag_run_id": "123456",
            "pipeline_args": {"id": "123", "flag": "True"},
        }
        return source_data, metadata

    def test_get_row_count(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Metrics):
            def metrics(self) -> List[MetricsData]:
                return GeMetrics(self.source_data).get_row_count().result()

        result = Identity(source_data=source_data, metadata=metadata).run()
        observed = result.select(
            "metrics",
            "results",
            "column_name",
            "column_value",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                metrics="get_row_count",
                results="10.0",
                column_name=None,
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_get_column_unique_count(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Metrics):
            def metrics(self) -> List[MetricsData]:
                return (
                    GeMetrics(self.source_data)
                    .get_column_unique_count("decision")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()
        observed = result.select(
            "metrics",
            "results",
            "column_name",
            "column_value",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                metrics="get_column_unique_count",
                results="2.0",
                column_name="decision",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_get_column_max(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Metrics):
            def metrics(self) -> List[MetricsData]:
                return (
                    GeMetrics(self.source_data)
                    .get_column_max("device_os_version")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()
        observed = result.select(
            "metrics",
            "results",
            "column_name",
            "column_value",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                metrics="get_column_max",
                results="14.0",
                column_name="device_os_version",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_get_column_min(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Metrics):
            def metrics(self) -> List[MetricsData]:
                return (
                    GeMetrics(self.source_data)
                    .get_column_min("device_os_version")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()
        observed = result.select(
            "metrics",
            "results",
            "column_name",
            "column_value",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                metrics="get_column_min",
                results="9.0",
                column_name="device_os_version",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_get_column_median(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Metrics):
            def metrics(self) -> List[MetricsData]:
                return (
                    GeMetrics(self.source_data)
                    .get_column_median("device_os_version")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()
        observed = result.select(
            "metrics",
            "results",
            "column_name",
            "column_value",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                metrics="get_column_median",
                results="10.0",
                column_name="device_os_version",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_get_column_stdev(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Metrics):
            def metrics(self) -> List[MetricsData]:
                return (
                    GeMetrics(self.source_data)
                    .get_column_stdev("device_os_version")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()
        observed = result.select(
            "metrics",
            "results",
            "column_name",
            "column_value",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                metrics="get_column_stdev",
                results="1.3984117975602017",
                column_name="device_os_version",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_get_column_count_in_range(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Metrics):
            def metrics(self) -> List[MetricsData]:
                return (
                    GeMetrics(self.source_data)
                    .get_column_count_in_range("device_os_version", min_val=1)
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()
        observed = result.select(
            "metrics",
            "results",
            "column_name",
            "column_value",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                metrics="get_column_count_in_range",
                results="10.0",
                column_name="device_os_version",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_get_rates(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Metrics):
            def metrics(self) -> List[MetricsData]:
                return GeMetrics(self.source_data).get_rates("decision", 10).result()

        result = Identity(source_data=source_data, metadata=metadata).run()
        observed = result.select(
            "metrics",
            "results",
            "column_name",
            "column_value",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                metrics="get_rate",
                results="0.6",
                column_name="decision",
                column_value="yes",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_rate",
                results="0.4",
                column_name="decision",
                column_value="no",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
        ]
        print(observed)
        assert sorted(observed) == sorted(expected)

    def test_get_column_values_count(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Metrics):
            def metrics(self) -> List[MetricsData]:
                return (
                    GeMetrics(self.source_data)
                    .get_column_values_count("decision")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()
        observed = result.select(
            "metrics",
            "results",
            "column_name",
            "column_value",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                metrics="get_column_value_count",
                results="4.0",
                column_name="decision",
                column_value="no",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_value_count",
                results="6.0",
                column_name="decision",
                column_value="yes",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
        ]
        print(observed)
        assert sorted(observed) == sorted(expected)
