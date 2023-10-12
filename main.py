from pathlib import Path

from app.pipeline import Pipeline
from app.utils.cmd import resolve_cmd_args
from app.utils.config import load_config, get_configs_from_directory
from app.utils.logger import customize_logging

import warnings

warnings.filterwarnings("ignore")

if __name__ == "__main__":
    options = resolve_cmd_args()

    path_to_configs = options.config

    try:
        for path_to_config in get_configs_from_directory(path_to_configs):
            config = load_config(path_to_config)

            logger = customize_logging(
                filepath=Path(config.logging.path),
                level=config.logging.level,
                retention=config.logging.retention,
                rotation=config.logging.rotation,
                _format=config.logging.format,
            )

            try:
                pipe = Pipeline(config, options, logger)
                pipe.start()
            except Exception as ex:
                logger.error(f"{path_to_config} : {ex}")

    except Exception as ex:
        print(ex)
