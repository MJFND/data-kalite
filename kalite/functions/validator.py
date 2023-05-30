from pyspark.sql.dataframe import DataFrame

from kalite.data_classes.result_data import ResultData


class Validator:
    @staticmethod
    def validate(data: DataFrame):
        validated = data.where(data[ResultData.NAME] == ResultData.FAIL).cache()
        count = validated.count()
        if count > 0:
            raise Exception(
                f"### {count} validations failed, Check logs or DWH Validation Table"
            )
