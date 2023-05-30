from typing import List

from kalite.data_classes.validations_data import ValidationsData
from kalite.functions.validations import GeValidations, Validations


class ConfigDrivenValidations(Validations):
    def validations(self) -> List[ValidationsData]:
        """
        An Overridden function to perform validations derived from a config dynamically

        :param data: A Dataframe
        :param config: Any dict config to derive validations
              template: {"<column_name>":
                            [
                                "<function_name>",
                                {"<function_name>": str | List[str] }
                            ]
                        }
              e.g: {"initial_decision":
                       [        # No extra param
                            "expect_column_to_exist",
                                # One extra param
                            {"expect_column_values_to_be_of_type": "StringType"},
                                # One extra params of type list
                            {"expect_column_values_to_be_in_set": ["X", "Y", "Z"]},
                       ]
                    }
              Practical usage in unit tests.

        :return: A list of dict
        """
        ge_validations = GeValidations(self.source_data)
        for column_name, checks in self.config.items():
            for check in checks:
                # Handle no column found error
                if (
                    column_name not in self.source_data.columns
                    and check != ge_validations.expect_column_to_exist.__name__
                ):
                    break
                # Runs functions that require an additional param
                if isinstance(check, dict):
                    # There will be exactly one {key: value} pair
                    key = list(check.keys())[0]
                    ge_validations.__getattribute__(key)(column_name, check.get(key))
                # Runs functions that does not require additional param
                else:
                    ge_validations.__getattribute__(check)(column_name)
        return ge_validations.result()
