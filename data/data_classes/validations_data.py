from dataclasses import dataclass


@dataclass
class ValidationsData:
    """
    Data Class which wraps the validation in standard Dataframe row.
    """

    validations: str
    results: str
    column_name: str
    ge_metadata: str = None
