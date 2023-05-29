from pyspark.sql.dataframe import DataFrame

from data.functions.validations import ResultData


class ValidationsFailureException:
    @staticmethod
    def validate(data: DataFrame):
        validated = data.where(data[ResultData.NAME] == ResultData.FAIL)

        if validated.count() > 0:
            raise Exception(
                "### Validation failed, check previous job logs 'analysed_processed' or Snowflake table"
            )
