import os
from typing import Dict, List, Tuple

import pytest
from pyspark.sql import DataFrame, Row, SparkSession

from kalite.data_classes.validations_data import ValidationsData
from kalite.functions.validations import GeValidations, Validations


class TestValidations:
    """
    Each unit test runs one metrics on same data
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
            .appName("Test Validations")
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

    def test_expect_column_to_exist(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_to_exist("decision")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()
        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()

        expected = [
            Row(
                validations="expect_column_to_exist",
                results="PASS",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_not_to_exist(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_to_exist("random_column")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_to_exist",
                results="FAIL",
                column_name="random_column",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_values_to_be_of_type(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_values_to_be_of_type("decision", "StringType")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_values_to_be_of_type",
                results="PASS",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_values_to_not_be_of_type(
        self, setup: Tuple[DataFrame, Dict]
    ):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_values_to_be_of_type("decision", "IntegerType")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_values_to_be_of_type",
                results="FAIL",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_values_to_be_in_set(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_values_to_be_in_set("decision", ["yes", "no"])
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_values_to_be_in_set",
                results="PASS",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_values_to_not_be_in_set(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_values_to_be_in_set("decision", ["maybe", "no"])
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_values_to_be_in_set",
                results="FAIL",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_values_to_not_be_null(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_values_to_not_be_null("id")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_values_to_not_be_null",
                results="PASS",
                column_name="id",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_values_to_be_null(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_values_to_not_be_null("type")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_values_to_not_be_null",
                results="FAIL",
                column_name="type",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_value_to_exist(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_value_to_exist("decision", "yes")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_value_to_exist",
                results="PASS",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_value_to_not_exist(self, setup: Tuple[DataFrame, Dict]):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_value_to_exist("decision", "maybe")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_value_to_exist",
                results="FAIL",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_values_to_not_belong_to_single_category(
        self, setup: Tuple[DataFrame, Dict]
    ):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_values_to_not_belong_to_single_category(
                        "decision", ["yes"]
                    )
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_values_to_not_belong_to_single_category",
                results="PASS",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_values_to_belong_to_single_category(
        self, setup: Tuple[DataFrame, Dict]
    ):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_values_to_not_belong_to_single_category(
                        "source", ["unknown"]
                    )
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_values_to_not_belong_to_single_category",
                results="FAIL",
                column_name="source",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_values_to_not_have_one_unique_count(
        self, setup: Tuple[DataFrame, Dict]
    ):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_values_to_not_have_one_unique_count("decision")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_values_to_not_have_one_unique_count",
                results="PASS",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_column_values_to_have_one_unique_count(
        self, setup: Tuple[DataFrame, Dict]
    ):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_column_values_to_not_have_one_unique_count("source")
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_column_values_to_not_have_one_unique_count",
                results="FAIL",
                column_name="source",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_custom_condition_result_count_to_be_zero(
        self, setup: Tuple[DataFrame, Dict]
    ):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_custom_condition_result_count_to_be_zero(
                        "decision", "== 'XYZ'"
                    )
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_custom_condition_result_count_to_be_zero",
                results="PASS",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected

    def test_expect_custom_condition_result_count_to_not_be_zero(
        self, setup: Tuple[DataFrame, Dict]
    ):
        source_data, metadata = setup

        class Identity(Validations):
            def validations(self) -> List[ValidationsData]:
                return (
                    GeValidations(self.source_data)
                    .expect_custom_condition_result_count_to_be_zero(
                        "decision", "== 'yes'"
                    )
                    .result()
                )

        result = Identity(source_data=source_data, metadata=metadata).run()

        observed = result.select(
            "validations",
            "results",
            "column_name",
            "dag_id",
            "dag_run_id",
            "dag_task_id",
        ).collect()
        expected = [
            Row(
                validations="expect_custom_condition_result_count_to_be_zero",
                results="FAIL",
                column_name="decision",
                dag_id="mydag",
                dag_run_id="123456",
                dag_task_id="mytask",
            )
        ]
        print(observed)
        assert observed == expected
