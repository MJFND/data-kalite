import json
from functools import wraps
from typing import Any, Dict, List

from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.dataset import SparkDFDataset
from pyspark.sql import DataFrame

from kalite.data_classes.result_data import ResultData
from kalite.data_classes.validations_data import ValidationsData
from kalite.functions.base import Base
from kalite.functions.metrics import GeMetrics
from kalite.utilities.logger import logger


class Validations(Base):
    def __init__(
        self, source_data: DataFrame, config: Any = None, metadata: Dict = None
    ):
        """
        Initialize
        :param source_data: A Dataframe holding the actual source data
        :param config: Any dict config to derive metrics
        :param metadata: Pipeline metadata parameters, e.g. run id extra args etc.
        """
        super(Validations, self).__init__(
            source_data=source_data,
            config=config,
            metadata=metadata,
        )

    @staticmethod
    def standardize(func: Any):
        """
        Decorator with wraps validations for logging and converting to row purposes
        :param func:
        :return: self (GeValidation Object)
        """

        @wraps(func)
        def inner(self, column, *args):
            validation_result = func(self, column, *args)
            results = ResultData.PASS if validation_result.success else ResultData.FAIL
            logger.info(
                f"Validations Result: Validation {func.__name__} is {results}.\n"
                f"User inputs: {column} {args}\n"
                f"Returned output: {validation_result.to_json_dict()}\n"
            )
            # this self is from ge_validation object
            self._check.append(
                ValidationsData(
                    column_name=column,
                    validations=func.__name__.strip("_"),
                    results=results,
                    ge_metadata=json.dumps(validation_result.to_json_dict()),
                )
            )
            return self

        return inner

    def run(self) -> DataFrame:
        """
        Runs the validations with schema handling and dataframe conversion

        :return:
        """
        return self.add_metadata_cols(
            self.to_df(
                data=self.validations(),
                schema=self.get_spark_schema(ValidationsData),
            )
        )


class GeValidations:
    def __init__(self, data: DataFrame):
        """
        Initialize the data as GE SparkDFDataset which persist data under the hood!
        Initialize the GeMetrics object which is used to calculate validations
        :param data:
        """
        self.ge_spark_df = SparkDFDataset(data)
        self.ge_metrics = GeMetrics(self.ge_spark_df.spark_df)
        self._check = []

    def result(self) -> List[ValidationsData]:
        """
        Returns the already list of validations data
        :return:
        """
        return self._check

    @Validations.standardize
    def expect_column_to_exist(self, column: str):
        """
        Expect Column to Exist in the registered dataframe
        :param column: Representing a column name on which we want to perform
        :return: Returns a self from the decorator
        """
        return self.ge_spark_df.expect_column_to_exist(column)

    @Validations.standardize
    def expect_column_values_to_be_of_type(self, column: str, datatype: str):
        """
        Expect Column Values to belong to one type
        :param column: Representing a column name on which we want to perform
        :param datatype: Expected spark data type to check, e.g StringType, IntegerType, MapType
                         More: https://spark.apache.org/docs/latest/sql-ref-datatypes.html
        :return: Returns a self from the decorator
        """
        return self.ge_spark_df.expect_column_values_to_be_of_type(column, datatype)

    @Validations.standardize
    def expect_column_values_to_be_in_set(self, column: str, value_set: List[Any]):
        """
        Expect Column Values to be in the given set
        :param column: Representing a column name on which we want to perform
        :param value_set: Expected Value set
        :return: Returns a self from the decorator
        """
        return self.ge_spark_df.expect_column_values_to_be_in_set(column, value_set)

    @Validations.standardize
    def expect_column_values_to_not_be_null(self, column: str):
        """
        Expect Column Values to not be null
        :param column: Representing a column name on which we want to perform
        :return: Returns a self from the decorator
        """
        return self.ge_spark_df.expect_column_values_to_not_be_null(column)

    """
        Custom Validations Start
        Wraps the validation as standard GE ExpectationSuiteValidationResult object
    """

    @Validations.standardize
    def expect_column_value_to_exist(self, column: str, value: str):
        """
        Expect Column Value to exist in the column
        :param column: Representing a column name on which we want to perform
        :param value: Expected value to check
        :return: Returns a self from the decorator
        """
        start_index = self.ge_metrics.find_last_calculated_metric_index()
        series = self.ge_metrics.get_column_values_count(column).result()[start_index:]
        success = False
        for i in series:
            condition = i.column_value == value
            if condition:
                success = condition
                break
        meta = series[0].__dict__ if series else None  # meta needs to be serialized
        return ExpectationSuiteValidationResult(success=success, meta=meta)

    @Validations.standardize
    def expect_column_values_to_not_belong_to_single_category(
        self, column: str, values: List[str]
    ):
        """
        Expect Column to not belong to the single category for the given column value(s)
            E.g Every If every record in `initial_decision` should not be  `auto reject`
            expect_column_values_to_not_belong_to_single_category('initial_decision', ['auto reject'])

        :param column: Representing a column name on which we want to perform
        :param values: Expected value or values
        :return: Returns a self from the decorator
        """
        values = list(map(lambda _: _.lower(), values))
        start_index = self.ge_metrics.find_last_calculated_metric_index()
        series = self.ge_metrics.get_column_values_count(column).result()[start_index:]
        # If it has one count and belongs to any one of the values
        success = not (len(series) == 1 and series[0].column_value in values)
        meta = series[0].__dict__ if series else None  # meta needs to be serialized
        return ExpectationSuiteValidationResult(success=success, meta=meta)

    @Validations.standardize
    def expect_column_values_to_not_have_one_unique_count(self, column: str):
        """
        Expect Column values to not have one unique count excluding Nulls
        :param column: Representing a column name on which we want to perform
        :return: Returns a self from the decorator
        """
        start_index = self.ge_metrics.find_last_calculated_metric_index()
        unique_count = (
            self.ge_metrics.get_column_unique_count(column)
            .result()[start_index]
            .results
        )
        # Fail if there is one unique count or `0` in case of all nulls
        success = not (unique_count <= 1)
        return ExpectationSuiteValidationResult(
            success=success, meta={"unique_count": unique_count}
        )

    @Validations.standardize
    def expect_custom_condition_result_count_to_be_zero(
        self, column: str, condition: str
    ):
        """
        Expect custom condition for running completely custom filters
        usage: expect_custom_condition_result_count_to_be_zero(
            dataframe, col(column_A) == XYZ and column_B is null
        )

        :param column: Column name used in the Left side of condition
        :param condition: Condition including the predicate and right side, e.g == 'XYZ'
        :return: Returns a self from the decorator
        """
        count = self.ge_spark_df.spark_df.filter(f"{column} {condition}").count()
        success = count == 0
        return ExpectationSuiteValidationResult(success=success, meta={"count": count})
