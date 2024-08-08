import dataclasses
import json
from dataclasses import dataclass
from enum import Enum
from typing import List

import dataconf
from pyhocon import ConfigTree


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


@dataclass
class Credentials(ConfigurationBase):
    pswd_token: str = ""


@dataclass
class AdditionalFilters(ConfigurationBase):
    tag: str = ""


@dataclass
class SyncOptions(ConfigurationBase):
    start_date: str = ""
    end_date: str = ""
    additional_filters: AdditionalFilters = dataclasses.field(default_factory=lambda: ConfigTree({}))


class LoadType(str, Enum):
    full_load = "full_load"
    incremental_load = "incremental_load"

    def is_incremental(self) -> bool:
        return self.value == self.incremental_load


@dataclass
class Destination(ConfigurationBase):
    load_type: LoadType = "incremental_load"


@dataclass
class Configuration(ConfigurationBase):
    authentication: Credentials
    endpoints: List[str]
    sync_options: SyncOptions
    destination: Destination
