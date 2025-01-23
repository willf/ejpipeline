from data_pipeline.etl.base import BaseETL
from data_pipeline import utils
from playwright.sync_api import sync_playwright


class EPAEjscreenDataETL(BaseETL):
    def __init__(self):
        self.directory_links = [
            "https://gaftp.epa.gov/EJScreen/",
        ]
        self.logger = utils.get_logger(__name__)

    @classmethod
    def etl_name(cls):
        return "epa_ejscreen_data"

    def extract(self):
        amt_downloaded = 0
        files_downloaded = 0
        for directory in self.directory_links:
            self.logger.info(f"Extracting data from {directory} for {self.etl_name()}")
            urls = []
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                urls = list(utils.ftp_like_download_list(directory, browser))
            for url in urls:
                if url.endswith("/"):
                    self.logger.info(f"Skipping directory {url}")
                    continue
                destination = self.save_source_path(
                    url.removeprefix("https://gaftp.epa.gov/")
                )
                if destination.exists():
                    self.logger.debug(f"{destination} already exists. Skipping.")
                    continue
                downloaded_amount = utils.download_url(url, destination, verify=False)
                # downloaded_amount = 0
                files_downloaded += 1

                amt_downloaded += downloaded_amount
            self.logger.info(
                f"Downloaded {self.etl_name()} data files to {destination}"
            )
        self.logger.info(
            f"Downloaded {files_downloaded} {self.etl_name()} data files to {destination}"
        )

    def transform(self):
        # self.logger.info(f"Transforming data for {self.etl_name()}")
        # Add transformation logic here
        pass

    def load(self):
        # self.logger.info(f"Loading data for {self.etl_name()}")
        # Add loading logic here
        pass


if __name__ == "__main__":
    etl = EPAEjscreenDataETL()
    etl.run(use_cache=True)
