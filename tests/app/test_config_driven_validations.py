import os
from typing import Dict, Tuple

import pytest
import yaml
from pyspark.sql import DataFrame, Row, SparkSession

from data.app.config_driven_validations import ConfigDrivenValidations


class TestConfigDrivenValidations:
    """
    Config Driven test, bulk test

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
            .appName("Test ConfigDrivenValidations")
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

        with open(
            os.path.dirname(os.path.realpath(__file__)) + "/../resources/config.yaml",
            "r",
        ) as file:
            config = yaml.safe_load(file)
        return source_data, metadata, config["validations"]

    def test_config_driven_validations(self, setup: Tuple[DataFrame, Dict, Dict]):
        source_data, metadata, config = setup
        observed = (
            ConfigDrivenValidations(
                source_data=source_data, metadata=metadata, config=config
            )
            .run()
            .select(
                "validations",
                "results",
                "column_name",
                "dag_id",
                "dag_run_id",
                "dag_task_id",
            )
            .collect()
        )

        expected = [
            Row(
                validations="expect_column_to_exist",
                results="PASS",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                validations="expect_column_values_to_be_of_type",
                results="PASS",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                validations="expect_column_values_to_be_in_set",
                results="PASS",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                validations="expect_column_value_to_exist",
                results="FAIL",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                validations="expect_column_values_to_not_be_null",
                results="PASS",
                column_name="id",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
            Row(
                validations="expect_column_to_exist",
                results="FAIL",
                column_name="random_column",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            ),
        ]

        print(observed)
        assert sorted(observed) == sorted(expected)
