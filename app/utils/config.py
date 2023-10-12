from typing import Dict, List, Optional, Literal
import yaml
from colorama import Fore, init
from pydantic import BaseSettings
import os


init(autoreset=True)


class LoggingConfig(BaseSettings):
    """
    Конфигурация логгирования (LOGURU)
    """

    level: str
    format: str
    retention: str
    rotation: str
    path: str


class BaseConfig:
    """
    Базовый класс для конфигурации по настройке pipeline
    """

    def __getitem__(self, item):
        return getattr(self, item)


class ChunkingConfig(BaseConfig, BaseSettings):
    """
    Настройки pipeline на чтение данных
    """

    threshold_to_chunk_MB: int = 500
    lines_per_chunked_file: int = 1000000


class ReadingConfig(BaseConfig, BaseSettings):
    """
    Настройки pipeline на чтение данных
    """

    type: Literal["csv", "json", "jsoneachrow", "excel"]
    path: str
    save_header: bool = False
    pandas_params: Optional[Dict] = dict()


class ProcessingConfig(BaseConfig, BaseSettings):
    """
    Настройки pipeline на обработку данных
    """

    mapping: Optional[Dict] = dict()
    fields_with_static_values: Optional[Dict] = dict()
    handlers: Optional[Dict] = dict()
    fields_to_drop: Optional[List] = list()
    fields_to_append: Optional[List] = list()


class ValuableFields(BaseConfig, BaseSettings):
    fields: Optional[List] = list()
    rule: Literal["strict", "loyal"] = "loyal"
    loyal_boundary_value: int = 1


class WritingConfig(BaseConfig, BaseSettings):
    """
    Настройки pipeline на запись данных
    """

    type: Literal["csv", "json", "jsoneachrow"]
    path: str
    path_to_processed: str
    path_to_errors: str
    valuable_fields: ValuableFields = dict()
    pandas_params: Optional[Dict] = dict()


class Config(BaseConfig, BaseSettings):
    """
    Полные настройки pipeline
    """

    chunking: ChunkingConfig
    reading: ReadingConfig
    processing: ProcessingConfig = None
    writing: WritingConfig
    logging: LoggingConfig


def get_configs_from_directory(configurations_path: str) -> List:
    method_name = get_configs_from_directory.__name__
    if not os.path.exists(configurations_path):
        raise Exception(
            (
                f"{configurations_path} : ERROR | {method_name} | Not found any configurations"
            )
        )

    if os.path.isdir(configurations_path):
        return [
            os.path.join(configurations_path, config_filename)
            for config_filename in os.listdir(configurations_path)
            if os.path.isfile(os.path.join(configurations_path, config_filename))
        ]
    else:
        return [configurations_path]


def load_config(configuration_path: str = "config.yaml") -> Config:
    """
    :param config_path: путь до конфигурации
    :return:
    """
    method_name = load_config.__name__

    with open(file=configuration_path, mode="rb") as fh:
        try:
            dictionary: Dict = yaml.load(stream=fh, Loader=yaml.FullLoader)
            config = Config.parse_obj(dictionary)
            return config
        except Exception as e:
            raise Exception(
                f"{Fore.RED}{configuration_path} : ERROR | {method_name} | {e}"
            )
