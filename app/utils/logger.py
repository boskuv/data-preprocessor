import logging
import sys
from pathlib import Path

from loguru import logger


class InterceptHandler(logging.Handler):
    loglevel_mapping = {
        50: "CRITICAL",
        40: "ERROR",
        30: "WARNING",
        20: "INFO",
        10: "DEBUG",
        0: "NOTSET",
    }

    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except AttributeError:
            level = self.loglevel_mapping[record.levelno]

        frame, depth = logging.currentframe(), 2
        while frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        log = logger.bind()
        log.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def customize_logging(
    filepath: Path, level: str, rotation: str, retention: str, _format: str
):
    """Define the logger to be used by the service based on loguru.
    Parameters:
        filepath: the path where to store the logfiles.
        level: the minimum log-level to log.
        rotation: when to rotate the logfile.
        retention: when to remove logfiles.
        _format: the logformat to use.
    Returns:
        the logger to be used by the service.
    """
    filepath.parent.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(
        sys.stdout, enqueue=True, backtrace=True, level=level.upper(), format=_format
    )
    logger.add(
        str(filepath),
        rotation=rotation,
        retention=retention,
        enqueue=True,
        backtrace=True,
        level=level.upper(),
        format=_format,
    )
    logging.basicConfig(handlers=[InterceptHandler()], level=0)
    return logger.bind()


__all__ = ["customize_logging"]
