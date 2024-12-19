import logging
from config import settings
from pathlib import Path


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)


def get_sink_path(etl_name: Path) -> Path:
    return settings.DATA_PATH / etl_name


def get_source_sink_path(etl_name: str) -> str:
    return get_sink_path(etl_name) / "sources"
