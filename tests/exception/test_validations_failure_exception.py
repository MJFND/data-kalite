import os
from typing import Tuple

import pytest
from pyspark.sql import DataFrame, SparkSession

from kalite.exception.validations_failure_exception import ValidationsFailureException


class TestValidationsFailureException:
    """
    Test the exception
    """

    @pytest.fixture(scope="session")
    def setup(self, request):
        """
        Fixture for creating a spark context and data.
        Used across all tests cases.
        """

        spark = (
            SparkSession.builder.master("local[1]")
            .appName("Test ValidationsFailureException")
            .getOrCreate()
        )
        # Last step to stop spark before the session ends
        request.addfinalizer(lambda: spark.stop())

        # Load test data
        df_reader = spark.read.format("csv").options(
            **{"header": "true", "inferSchema": "true"}
        )
        passed_validations = df_reader.load(
            os.path.dirname(os.path.realpath(__file__))
            + "../../resources/passed_validations.csv"
        )
        failed_validations = df_reader.load(
            os.path.dirname(os.path.realpath(__file__))
            + "../../resources/failed_validations.csv"
        )

        return passed_validations, failed_validations

    def test_passed_validations(self, setup: Tuple[DataFrame, DataFrame]):
        passed_validations, _ = setup
        try:
            ValidationsFailureException.validate(passed_validations)
        except Exception:
            assert False
        else:
            assert True

    def test_failed_validations(self, setup: Tuple[DataFrame, DataFrame]):
        _, failed_validations = setup
        try:
            ValidationsFailureException.validate(failed_validations)
        except Exception:
            assert True
        else:
            assert False
