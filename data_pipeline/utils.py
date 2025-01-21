import logging
from config import settings
from pathlib import Path
import importlib
import inspect
from rich.logging import RichHandler
from playwright.sync_api import sync_playwright
import re
import requests
import urllib3

from rich.progress import (
    SpinnerColumn,
    DownloadColumn,
    Progress,
    TransferSpeedColumn,
    TimeElapsedColumn,
)


def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        ch = RichHandler()
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


def get_classes_from_module(module, superclass=None):
    classes = [obj for name, obj in inspect.getmembers(module, inspect.isclass)]
    if superclass:
        classes = [cls for cls in classes if issubclass(cls, superclass)]
    return classes


def etl_classes(directory: Path, base_class: type):
    logger = get_logger(__name__)
    for thing in directory.iterdir():
        if thing.is_dir():
            # check if there is a file called etl.py
            etl_file = thing / "etl.py"
            if etl_file.exists():
                # given the caller, import the module
                package = base_class.__module__
                # remove 'base' from the package name
                package = package[: package.rfind(".")]
                module = importlib.import_module(f".{thing.name}.etl", package=package)
                classes = get_classes_from_module(module, base_class)
                for cls in classes:
                    logger.debug(f"Imported {cls}")
                    if cls.__name__ == "BaseETL":
                        continue
                    yield cls


## Download functions


def download_url(
    url: str, destination: Path, estimated_size=None, force=True, verify=True
) -> int:
    if destination.exists() and not force:
        return 0
    if not verify:
        urllib3.disable_warnings()
    response = requests.get(url, stream=True, verify=verify)
    # if it returns a 404, raise an exception
    response.raise_for_status()
    # ensure the destination directory exists
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Error creating directory {destination.parent}")
        print(e)
    # only used if the server doesn't return a content-length header
    # in that case, we'll just assume the file is 1GB
    estimated_size = estimated_size or 1024**3
    total = int(response.headers.get("content-length", estimated_size))
    bytes_so_far = 0
    if total == 0:
        progress_columns = [
            SpinnerColumn(),
            TimeElapsedColumn(),
            TransferSpeedColumn(),
            "[progress.description]{task.description}",
        ]
    else:
        progress_columns = [
            SpinnerColumn(),
            TimeElapsedColumn(),
            DownloadColumn(binary_units=True),
            TransferSpeedColumn(),
            "[progress.description]{task.description}",
        ]
    with Progress(*progress_columns) as progress:
        task = progress.add_task(f"Downloading {url} to {destination}", total=total)
        with open(destination, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
                bytes_so_far += len(chunk)
                progress.update(task, advance=len(chunk))
    return bytes_so_far


def download_via_download(url: str, destination: Path, estimated_size=None) -> int:
    with sync_playwright() as p:
        print("Launching browser")
        browser = p.chromium.launch(headless=True)
        print("Creating new page")
        page = browser.new_page()

        print("Expecting download")
        with page.expect_download() as download_info:
            print(f"Navigating to URL... {url}")
            page.goto(url)
            print("Performing click")
            download = download_info.value
        # Wait for the download process to complete and save the downloaded file somewhere
        download.save_as(destination)
        return 1


def ftp_like_download_list(url: str, browser):
    """
    Given a URL that looks like a directory listing, yield a list, recursively, of all files
    """
    logger = get_logger(__name__)
    # get the page, if it doesn't exist, stop
    logger.info(f"Reading FTP page at {url}")
    page = browser.new_page()
    page.goto(url)
    # parse the page, getting all the links
    links = re.findall(r'href="(.*?)"', page.content())
    # for each link, if it's a directory, recurse
    # if it's a file, yield it

    for link in links:
        # remove any query params
        link = link.split("?")[0]
        if link.endswith("/"):
            logger.info(f"Recursing into {link}")
            yield from ftp_like_download_list(url + link, browser)
        else:
            logger.info(f"Yielding {url + link}")
            yield url + link


def state_fips_code(state: str) -> str:
    state_fips = {
        "Alabama": "01",
        "Alaska": "02",
        "Arizona": "04",
        "Arkansas": "05",
        "California": "06",
        "Colorado": "08",
        "Connecticut": "09",
        "Delaware": "10",
        "District of Columbia": "11",
        "Florida": "12",
        "Georgia": "13",
        "Hawaii": "15",
        "Idaho": "16",
        "Illinois": "17",
        "Indiana": "18",
        "Iowa": "19",
        "Kansas": "20",
        "Kentucky": "21",
        "Louisiana": "22",
        "Maine": "23",
        "Maryland": "24",
        "Massachusetts": "25",
        "Michigan": "26",
        "Minnesota": "27",
        "Mississippi": "28",
        "Missouri": "29",
        "Montana": "30",
        "Nebraska": "31",
        "Nevada": "32",
        "New Hampshire": "33",
        "New Jersey": "34",
        "New Mexico": "35",
        "New York": "36",
        "North Carolina": "37",
        "North Dakota": "38",
        "Ohio": "39",
        "Oklahoma": "40",
        "Oregon": "41",
        "Pennsylvania": "42",
        "Rhode Island": "44",
        "South Carolina": "45",
        "South Dakota": "46",
        "Tennessee": "47",
        "Texas": "48",
        "Utah": "49",
        "Vermont": "50",
        "Virginia": "51",
        "Washington": "53",
        "West Virginia": "54",
        "Wisconsin": "55",
        "Wyoming": "56",
        "American Samoa": "60",
        "Guam": "66",
        "Northern Mariana Islands": "69",
        "Puerto Rico": "72",
        "Virgin Islands": "78",
    }

    return state_fips.get(state, "Unknown")
