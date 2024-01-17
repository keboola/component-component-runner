import dataclasses
import json
from dataclasses import dataclass, field
from typing import List, Dict
from enum import Enum
import dataconf


class ConfigurationBase:
    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith("pswd_"):
            return value.replace("pswd_", "#", 1)
        else:
            return value

    @classmethod
    def load_from_dict(cls, configuration: dict):
        """
        Initialize the configuration dataclass object from dictionary.
        Args:
            configuration: Dictionary loaded from json configuration.

        Returns:

        """
        json_conf = json.dumps(configuration)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return dataconf.loads(json_conf, cls, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name)
                for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING
                ]


class VariableMode(str, Enum):
    self_defined = "self_defined"
    from_file_run_all = "from_file_run_all"
    from_file_run_first = "from_file_run_first"


@dataclass
class ComponentParameters(ConfigurationBase):
    pswd_sapi_token: str = ""
    component_id: str = ""
    config_id: str = ""
    keboola_stack: str = ""
    custom_stack: str = ""


@dataclass
class RunParameters(ConfigurationBase):
    variable_mode: VariableMode = VariableMode.self_defined
    wait_until_finish: bool = False
    use_variables: bool = False
    variables: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class MatrixParameters(ConfigurationBase):
    annotations: List[str] = field(default_factory=list)


@dataclass
class Configuration(ConfigurationBase):
    component_parameters: ComponentParameters
    run_parameters: RunParameters
