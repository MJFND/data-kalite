## Data-Kalite
[![PyPI version](https://badge.fury.io/py/data-kalite.svg)](https://badge.fury.io/py/data-kalite)

Kalite means Quality in Turkish. </br>
This is a framework on top of open source Great Expectations PySpark Suite with additional features.

About Great Expectations:
- It supports Spark 3.0
- It has built in Metrics and Validations Suite that provide information
- [Optional] Can be hooked up with the GE HTML static pages

On Top of GE:
- Ability to return a standardized DataFrame for both Metrics and Validations that can be pushed to DWH for historical analysis
- Runs all validations first before failing the pipeline
    - Returns a Validation generated standardized DataFrame
    - Failing the pipeline when one of the validation has failed
    - Logs validation through logger
- Support to add metadata column for tracking purposes (dag_id etc)
- Support to pass optional pipeline args (batch_date etc)
- Deriving validations through configurations (YAML configs or Dict)
- Easy to run default metrics
- Ability to use lower level api for metrics and validations

## Usage
Requires: `3.7.0 < Python < 3.10.0` <br/>
Few ways:
- Download from Pypi `pip install data-kalite`
- Clone and integrate in your codebase
- Clone and build your python package:
    - `pip install .` for local install
    - `python setup.py bdist_wheel` for distribution

### Metrics
How to run metrics: </br>
Read: [Limitations](https://github.com/MJFND/data-kalite#limitations)</br>
To run default metrics;
```python
from kalite.application.default_metrics import DefaultMetrics

source_data = "<set_dataframe>"
metadata = {
    "dag_id": "<set>",
    "dag_task_id": "<set>",
    "dag_run_id": "<set>",
    "pipeline_args": {"<set>": "<set>"}, # OPTIONAL
}
# run() returns a DataFrame
# set unique_count_threshold depending on data size: Details [here](https://github.com/MJFND/data-kalite/blob/main/kalite/functions/metrics.py#L27-L29) 
metrics = DefaultMetrics(source_data=source_data, metadata=metadata, unique_count_threshold=5).run()
metrics.show(10,False)
```

Implementing Metrics:
```python
from typing import List
from kalite.data_classes.metrics_data import MetricsData
from kalite.functions.metrics import GeMetrics, Metrics

class TempM(Metrics):
    def metrics(self) -> List[MetricsData]:
        return GeMetrics(self.source_data).\
            get_column_unique_count("column_A").\
            get_column_max("column_B").\
            result()
    
source_data = "<set_dataframe>"
metadata = {
    "dag_id": "<set>",
    "dag_task_id": "<set>",
    "dag_run_id": "<set>",
    "pipeline_args": {"<set>": "<set>"}, # OPTIONAL
}
# run() returns a DataFrame
# set unique_count_threshold depending on data size: Details [here](https://github.com/MJFND/data-kalite/blob/main/kalite/functions/metrics.py#L27-L29) 
metrics = TempM(source_data=source_data, metadata=metadata, unique_count_threshold=2).run()
metrics.show(10,False)
```
Refer to unit test cases on usage.

Metrics sample result: 

|id                              |created_at             |pipeline_args            |dag_id|dag_run_id|dag_task_id|metrics                |results|column_name      |column_value|
|--------------------------------|-----------------------|-------------------------|------|----------|-----------|-----------------------|-------|-----------------|------------|
|53d924311faca5490a94d6f2d51ae7d6|2023-05-29 23:12:35.351|{id -> 123, flag -> True}|mydag |123456    |mytask     |get_column_unique_count|0.0    |event_date       |null        |
|9c209f282a8a91f4a2c07b59406e68cf|2023-05-29 23:12:35.351|{id -> 123, flag -> True}|mydag |123456    |mytask     |get_column_value_count |10.0   |source           |unknown     |
|5f57c0104e1fab302b134bac84b7c2d2|2023-05-29 23:12:35.351|{id -> 123, flag -> True}|mydag |123456    |mytask     |get_column_median      |10.0   |device_os_version|null        |
|afc8be1f1a2fc296f2133ebc1cca45f7|2023-05-29 23:12:35.351|{id -> 123, flag -> True}|mydag |123456    |mytask     |get_rate               |0.9    |device_os_name   |Android     |
|3d10cc1b98a617e54a8af6eed33677d4|2023-05-29 23:12:35.351|{id -> 123, flag -> True}|mydag |123456    |mytask     |get_column_unique_count|0.0    |type             |null        |


### Validations
How to run validations: </br>
Read: [Limitations](https://github.com/MJFND/data-kalite#limitations)</br>
To run validations, `validations` function must be implemented.
```python
from typing import List
from kalite.data_classes.validations_data import ValidationsData
from kalite.functions.validations import GeValidations, Validations

class TempV(Validations):
    def validations(self) -> List[ValidationsData]:
        return GeValidations(self.source_data).\
            expect_column_to_exist("column_A").\
            expect_column_values_to_be_of_type("column_A", "StringType").\
            expect_column_value_to_exist("column_A", "XYZ").\
            expect_column_values_to_not_have_one_unique_count("column_A").\
            result()

# source_data = "<set_dataframe>"
metadata = {
    "dag_id": "<set>",
    "dag_task_id": "<set>",
    "dag_run_id": "<set>",
    "pipeline_args": {"<set>": "<set>"}, # OPTIONAL
}
# run() returns a DataFrame
validations = TempV(source_data=source_data, metadata=metadata).run()
validations.show(10,False)
```
Refer to unit test cases on usage.

### Config Driven Validations
A simple validator service on top of validations that allows to perform validations via a config dynamically.
A config should be in a standard `Dict` format as shown under`config_driven_validations.py`.

Calling the function is easy, just pass config param as Dict, could be derived from a YAML or JSON. Benefits of decoupling from code is you can keep these validations in a centralized Data Contract repository.

YAML Config:
```yaml
validations:
    <column_name>:
        - validation_1
        - validation_2
        .
        .
        - validation_n
```
Refer to unit test resource for full example: [link](https://github.com/MJFND/Data-Kalite/blob/main/tests/resources/config.yaml)

Function require config to be dictionary, it can be sourced from YAML or JSON as long as structure is followed.
```python
from kalite.application.config_driven_validations import ConfigDrivenValidations

source_data = "<set_dataframe>"
metadata = {
    "dag_id": "<set>",
    "dag_task_id": "<set>",
    "dag_run_id": "<set>",
    "pipeline_args": {"<set>": "<set>"}, # OPTIONAL
}
config = "<set_dict>" # Details: https://github.com/MJFND/data-kalite/blob/main/kalite/application/config_driven_validations.py#L20-L28

# run() returns a DataFrame
validations = ConfigDrivenValidations(source_data=source_data, metadata=metadata, config=config).run()
validations.show(10,False)
```
Refer to unit test cases on usage.

Validation sample result: 

|id                              |created_at             |pipeline_args                                                 |dag_id|dag_run_id|dag_task_id|validations                        |results|column_name  |ge_metadata                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|--------------------------------|-----------------------|--------------------------------------------------------------|------|----------|-----------|-----------------------------------|-------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|40a2037a2298c8a86aa9c5c92c3a9db9|2023-05-29 16:20:32.975|{id -> 123, flag -> True}|mydag |123456    |mytask     |expect_column_to_exist             |PASS   |decision     |{"meta": {}, "result": {}, "success": true, "expectation_config": {"meta": {}, "kwargs": {"column": "decision", "result_format": "BASIC"}, "expectation_type": "expect_column_to_exist"}, "exception_info": {"raised_exception": false, "exception_traceback": null, "exception_message": null}}                                                                                                                                                                                                                                                              |
|f5af2ed95a5ac827cf7d5020e44fee6e|2023-05-29 16:20:32.975|{id -> 123, flag -> True}|mydag |123456    |mytask     |expect_column_values_to_be_of_type |PASS   |decision     |{"meta": {}, "result": {"observed_value": "StringType"}, "success": true, "expectation_config": {"meta": {}, "kwargs": {"column": "decision", "type_": "StringType", "result_format": "BASIC"}, "expectation_type": "expect_column_values_to_be_of_type"}, "exception_info": {"raised_exception": false, "exception_traceback": null, "exception_message": null}} 

### Failing the Task
After generating the DataFrame of validations, `Validator` can be used to throw exception to fail pipeline.
```python
from kalite.functions.validator import Validator

validation_data = "<set_dataframe>" # dataframe that was generated through Validations
Validator.validate(validation_data)
```

## Limitations
Data-Kalite does not support `DATE` or `TIMESTAMP` datatype input columns for certain metrics that requires `column_value` to be set. </br>
E.g. Metric `get_column_values_count`, See metrics [here](https://github.com/MJFND/data-kalite/blob/main/kalite/functions/metrics.py#L117-L253)
- `column_value` is casted as `STRING` during `DataFrame` generation, casting can fail if input types are `DATE` or `TIMESTAMP`.

Since Spark column can be of one type, workaround is to explicitly convert these into `STRING` before running metrics or avoid running these checks for those types of columns.


## Contributing
#### Setup Local Environment
Using `makefile` to keep things simple.

- `make install`
    - test via:
        - `make pre-commit`
        - `git commit`

* To learn more about `pre-commit` tools check the `.pre-commit-config.yaml` in root.

### Metrics
How to add new metrics:
- All metrics must start with `get_`
- All metrics must be wrapped in a `@Metrics.standardize` function.
- All metrics must return a `MetricsSuiteResult`
- Most of the metrics are abstraction on top of GE as mentioned [here](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/dataset/sparkdf_dataset.py#L632).

Refer to existing function in `metrics.py`

### Validations
How to add new validations:
- All expectations must start with `expect_`
- All expectations must be wrapped in a `@Validations.standardize` function.
- All expectations must return a `ExpectationSuiteValidationResult`
- Most of the validations are abstraction on top of GE as mentioned [here](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/dataset/sparkdf_dataset.py#L844).

Refer to existing function in `validations.py`
