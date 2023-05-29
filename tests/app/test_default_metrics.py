import os
from typing import Dict, Tuple

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from data.app.default_metrics import DefaultMetrics


class TestDefaultMetrics:
    """
    Default Metrics, bulk test

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
            .appName("Test Default Metrics")
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
        metadata = {"dag_id": "mydag", "dag_task_id": "mytask", "dag_run_id": "123456", "pipeline_args": {"id": "123", "flag": "True"}}
        return source_data, metadata

    def test_default_metrics(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup
        default_metrics = DefaultMetrics(source_data=source_data, metadata=metadata)
        # Overwriting for a smaller set
        default_metrics.unique_count_threshold = 2
        observed = (
            default_metrics.run()
            .select(
                "metrics",
                "results",
                "column_name",
                "column_value",
                "dag_id",
                "dag_run_id",
                "dag_task_id",
            )
            .collect()
        )
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
                metrics="get_rate",
                results="0.9",
                column_name="device_os_name",
                column_value="Android",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_median",
                results="0.0",
                column_name="result",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_unique_count",
                results="10.0",
                column_name="id",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_value_count",
                results="9.0",
                column_name="device_os_name",
                column_value="Android",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_median",
                results="10.0",
                column_name="device_os_version",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_rate",
                results="1.0",
                column_name="source",
                column_value="unknown",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_unique_count",
                results="2.0",
                column_name="decision",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_max",
                results="14.0",
                column_name="device_os_version",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_min",
                results="1.0",
                column_name="id_length",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_rate",
                results="0.1",
                column_name="device_os_name",
                column_value="IOS",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_unique_count",
                results="1.0",
                column_name="source",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_row_count",
                results="10.0",
                column_name=None,
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_unique_count",
                results="2.0",
                column_name="device_os_name",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_median",
                results="1.0",
                column_name="id_length",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_unique_count",
                results="5.0",
                column_name="device_manufacturer",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_value_count",
                results="10.0",
                column_name="source",
                column_value="unknown",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
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
                metrics="get_column_min",
                results="9.0",
                column_name="device_os_version",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_min",
                results="0.0",
                column_name="result",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_unique_count",
                results="10.0",
                column_name="device_id",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_unique_count",
                results="10.0",
                column_name="ip",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_unique_count",
                results="0.0",
                column_name="event_date",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_max",
                results="0.0",
                column_name="result",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_max",
                results="1.0",
                column_name="id_length",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_value_count",
                results="1.0",
                column_name="device_os_name",
                column_value="IOS",
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
            Row(
                metrics="get_column_unique_count",
                results="10.0",
                column_name="device_model",
                column_value=None,
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                metrics="get_column_unique_count",
                results="0.0",
                column_name="type",
                column_value=None,
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
