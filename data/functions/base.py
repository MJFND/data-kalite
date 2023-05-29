from abc import ABC, abstractmethod
from itertools import chain
from time import time
from typing import Any, Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    concat_ws,
    create_map,
    current_timestamp,
    lit,
    md5,
    udf,
)
from pyspark.sql.types import StringType, StructField, StructType

from data.data_classes.meta_data import MetaData
from data.data_classes.metrics_data import MetricsData
from data.data_classes.validations_data import ValidationsData
from data.utilities.logger import logger


class Base(ABC):
    def __init__(
        self,
        source_data: DataFrame,
        config,
        metadata: Dict,
        generate_metadata,
        unique_count_threshold: int = 25,
    ):
        """
        Initialize
        :param source_data: A Dataframe holding the actual source data
        :param config: Any dict config to derive metrics
        :param metadata: Pipeline metadata parameters, e.g. run id extra args etc.
        :param generate_metadata: Bool to find if metadata is needed or not (contains: id, created_at, pipeline_args)
        :param unique_count_threshold: Threshold used to calculate default metrics.
                             Defaults to 25, columns that have unique_count less than or equal to 25 will
                             calculate value level metrics like get_rate
        """

        self.metadata = metadata
        self.source_data = source_data
        self.config = config
        self.generate_metadata = generate_metadata
        self.unique_count_threshold = unique_count_threshold

    def metrics(self) -> List[MetricsData]:
        """
        Core function for metrics to be overridden

        :return: A list of MetricsData
        """
        logger.warning("No metrics defined")
        return []

    def validations(self) -> List[ValidationsData]:
        """
        Core function for validations to be overridden

        :return: A list of ValidationsData
        """
        logger.warning("No validation defined")
        return []

    @abstractmethod
    def run(self) -> DataFrame:
        """
        Runs the metrics and Validations within their classes
        :return: A dataframe
        """

    @staticmethod
    def to_df(data: List[Any], schema: StructType) -> DataFrame:
        """
        Converts a list of objects to a dataframe with schema
        :param data: A dataclass object either Metrics or Validation
        :param schema
        :return:
        """
        data = list(map(lambda _: _.__dict__, data))
        return SparkSession.builder.getOrCreate().createDataFrame(data, schema)

    def add_metadata_cols(self, data: DataFrame) -> DataFrame:
        """
        Adds the default metadata columns
        :param data: A DataFrame that has been converted from a List of dict
        :return: A dataframe with metadata columns
        """
        optional_cols = []
        required_cols = [MetaData.dag_id, MetaData.dag_run_id, MetaData.dag_task_id]
        base_cols = data.columns
        if self.generate_metadata:
            optional_cols = [
                MetaData.id,
                MetaData.created_at,
                MetaData.pipeline_args,
            ]
            data = (
                data.withColumn(
                    MetaData.id, md5(concat_ws("", *data.columns, udf(time)()))
                )
                .withColumn(MetaData.created_at, lit(current_timestamp()))
                .withColumn(
                    MetaData.pipeline_args,
                    create_map(*[lit(x) for x in chain(*self.metadata.get('pipeline_args', {}).items())]),
                )
            )

        data = (
            data.withColumn(
                MetaData.dag_id, lit(self.metadata.get(MetaData.dag_id, None))
            )
            .withColumn(
                MetaData.dag_run_id, lit(self.metadata.get(MetaData.dag_run_id, None))
            )
            .withColumn(
                MetaData.dag_task_id, lit(self.metadata.get(MetaData.dag_task_id, None))
            )
        )

        # Order the final dataframe
        final_cols = optional_cols + required_cols + base_cols
        return data.select(*final_cols)

    @staticmethod
    def standardize(func: Any):
        """
        Wrapper function to be overridden and used as a decorator
        within child classes that standardize output as Object
        :param func:
        :return:
        """

    @staticmethod
    def get_spark_schema(dataclass: Any) -> StructType:
        """
        Dynamically takes the dataclass variables and creates a spark schema
        we keep everything as string except dict as maptypes for simplicity and dynamic nature of columns

        Spark schema is required to handle correct types like None to
        null automatic mapping

        :return:
        """
        var = vars(dataclass).get("__annotations__")
        struct = []
        for k in var.keys():
            struct.append(StructField(k, StringType(), True))
        return StructType(struct)
