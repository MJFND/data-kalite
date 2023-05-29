## Data Kalite
Kalite means Quality in Turkish.
This is a framework on top of open source Great Expectations PySpark Suite with additional features.

About Great Expectations:
- It supports Spark 3.0
- It has built in Metrics and Validations Suite that provide information
- [Optional] Can be hooked up with the GE HTML static pages

On Top of GE:
- Runs all validations first before failing the pipeline
    - Returns a Validation generated standardized DataFrame that can be pushed to DWH for historical analysis
    - Failing the pipeline when one of the validation has failed
    - Logs validation through logger
- Support to add metadata column for tracking purposes (dag_id etc)
- Deriving validations through configurations (YAML configs or Dict)
- Easy to run default metrics with option to overwrite

## Usage
### Metrics
How to run metrics: </br>
To run default metrics;
```
class TempM(Metrics):
    # overwrite metrics function
    pass

# run function returns a dataframe    
# data is dataframe, metadata is a dict of metadata columns
metrics = TempM(source_data=data, metadata=metadata).run()
```
Refer to unit test cases on usage.

Metrics sample result: 

|metrics                |results|column_name      |column_value|dag_id|dag_run_id|dag_task_id|
|-----------------------|-------|-----------------|------------|------|----------|-----------|
|get_column_unique_count|0.0    |event_date       |null        |mydag |123456    |mytask     |
|get_column_value_count |10.0   |source           |unknown     |mydag |123456    |mytask     |
|get_column_median      |10.0   |device_os_version|null        |mydag |123456    |mytask     |
|get_rate               |0.9    |device_os_name   |Android     |mydag |123456    |mytask     |
|get_column_unique_count|0.0    |type             |null        |mydag |123456    |mytask     |


### Validations
How to run validations: </br>
To run validations, `validations` function must be implemented.
```
class TempV(Validations):
    def validations(self) -> List[ValidationsData]:
        return GeValidations(self.source_data).\
            expect_column_to_exist("decision").\
            expect_column_values_to_be_of_type(column, "StringType").\
            expect_column_value_to_exist("decision", "XYZ").\
            expect_column_values_to_not_have_one_unique_count("decision").\
            result()

# run function returns a dataframe
# data is dataframe, metadata is a dict of metadata columns
validations = TempV(source_data=data, metadata=metadata).run()
```
Refer to unit test cases on usage.

### Config Driven Validations
A simple validator service on top of validations that allows to perform validations via a config dynamically.
A config should be in a standard `Dict` format as shown under`config_driven_validations.py`.

Calling the function is easy, just pass optional config param.
YAML Config:
```
validations:
    <column_name>:
        - validation_1
        - validation_2
        .
        .
        - validation_n
```
Refer to unit test resource for full example: Link.

Function require config to be dictionary, it can be sourced from YAML or JSON as long as structure is followed.
```
# data is dataframe, metadata is a dict of metadata columns, config is validations
ConfigDrivenValidations(source_data=data, metadata=metadata, config=config).run()
```
Refer to unit test cases on usage.

Validation sample result: 

|id                              |created_at             |pipeline_args                                                 |dag_id|dag_run_id|dag_task_id|validations                        |results|column_name  |ge_metadata                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|--------------------------------|-----------------------|--------------------------------------------------------------|------|----------|-----------|-----------------------------------|-------|-------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
|40a2037a2298c8a86aa9c5c92c3a9db9|2023-05-29 16:20:32.975|{dag_id -> mydag, dag_task_id -> mytask, dag_run_id -> 123456}|mydag |123456    |mytask     |expect_column_to_exist             |PASS   |decision     |{"meta": {}, "result": {}, "success": true, "expectation_config": {"meta": {}, "kwargs": {"column": "decision", "result_format": "BASIC"}, "expectation_type": "expect_column_to_exist"}, "exception_info": {"raised_exception": false, "exception_traceback": null, "exception_message": null}}                                                                                                                                                                                                                                                              |
|f5af2ed95a5ac827cf7d5020e44fee6e|2023-05-29 16:20:32.975|{dag_id -> mydag, dag_task_id -> mytask, dag_run_id -> 123456}|mydag |123456    |mytask     |expect_column_values_to_be_of_type |PASS   |decision     |{"meta": {}, "result": {"observed_value": "StringType"}, "success": true, "expectation_config": {"meta": {}, "kwargs": {"column": "decision", "type_": "StringType", "result_format": "BASIC"}, "expectation_type": "expect_column_values_to_be_of_type"}, "exception_info": {"raised_exception": false, "exception_traceback": null, "exception_message": null}} 

### Failing the Task
After generating the DataFrame of validations, `ValidationsFailureException` can be used to throw exception to fail pipeline.

## Contributing
#### Setup Local Environment
Using `makefile` to keep things simple.

- `make install`
    - test via:
        - `make pre-commit`
        - `git commit`

* To learn more about `pre-commit` tools check the `.pre-commit-config.yaml` in root.

###Metrics
How to add new metrics:
- All metrics must start with `get_`
- All metrics must be wrapped in a `@Metrics.standardize` function.
- All metrics must return a `MetricsSuiteResult`
- Most of the metrics are abstraction on top of GE as mentioned [here](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/dataset/sparkdf_dataset.py#L632).

Refer to existing function in `metrics.py`

###Validations
How to add new validations:
- All expectations must start with `expect_`
- All expectations must be wrapped in a `@Validations.standardize` function.
- All expectations must return a `ExpectationSuiteValidationResult`

- Most of the validations are abstraction on top of GE as mentioned [here](https://github.com/great-expectations/great_expectations/blob/develop/great_expectations/dataset/sparkdf_dataset.py#L844).

Refer to existing function in `validations.py`