from dataclasses import dataclass


@dataclass
class ResultData:
    """
    Result data
    """

    NAME: str = "results"
    PASS: str = "PASS"
    FAIL: str = "FAIL"
