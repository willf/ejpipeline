import logging
from config import settings
from pathlib import Path
import importlib
import inspect


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
    return logger


def get_etl_path(etl_name: str) -> Path:
    return settings.DATA_PATH / etl_name


def get_sink_path(etl_name: Path) -> Path:
    return settings.DATA_PATH / etl_name / "sinks"


def get_source_sink_path(etl_name: str) -> Path:
    return settings.DATA_PATH / etl_name / "sources"


def get_temp_path(etl_name: str) -> Path:
    return settings.DATA_PATH / etl_name / "temp"


def get_classes_from_module(module):
    classes = [obj for name, obj in inspect.getmembers(module, inspect.isclass)]
    return classes


def etl_classes(directory: Path):
    logger = get_logger(__name__)
    for thing in directory.iterdir():
        if thing.is_dir():
            # check if there is a file called etl.py
            etl_file = thing / "etl.py"
            if etl_file.exists():
                logger.debug(f"Importing {etl_file}")
                module = importlib.import_module(f"{thing.name}.etl")
                classes = get_classes_from_module(module)
                for cls in classes:
                    logger.debug(f"Imported {cls}")
                    if cls.__name__ == "BaseETL":
                        continue
                    yield cls
