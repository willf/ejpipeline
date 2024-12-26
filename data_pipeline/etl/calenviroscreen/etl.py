from data_pipeline.etl.base import BaseETL

from data_pipeline import utils
from playwright.sync_api import sync_playwright


class CalEnviroScreenETL(BaseETL):
    def __init__(self):
        self.screen_page = (
            "https://oehha.ca.gov/calenviroscreen/report/calenviroscreen-40"
        )
        self.screen_excel_and_dd_text = (
            "CalEnviroScreen 4.0 Excel and Data Dictionary PDF"
        )
        self.shapefile_text = "CalEnviroScreen 4.0 SHP file"
        self.df = None
        self.logger = utils.get_logger(__name__)

    def etl_name(self):
        return "calenviroscreen"

    def already_etled(self):
        destination = utils.get_source_sink_path(self.etl_name())
        return destination.exists() and len(list(destination.iterdir())) > 0

    def extract(self):
        self.logger.info(f"Extracting data from {self.etl_name()}")
        with sync_playwright() as p:
            self.logger.debug(f"Extracting data from {self.screen_page}")
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            self.logger.debug(f"Navigating to URL... {self.screen_page}")
            page.goto(self.screen_page)
            # Start waiting for the first download
            with page.expect_download() as download_info:
                # Perform the action that initiates download
                self.logger.debug(f"Clicking on {self.screen_excel_and_dd_text}")
                loc = (
                    page.locator("strong")
                    .filter(has_text=self.screen_excel_and_dd_text)
                    .get_by_role("link")
                )
                self.logger.debug(f"Clicking on {loc}")
                loc.click()
            download = download_info.value
            # Wait for the download process to complete and save the downloaded file somewhere
            destination = self.save_source_path(download.suggested_filename)
            download.save_as(destination)
            self.logger.info(
                f"Downloaded {self.etl_name()} data files to {destination}"
            )
            with page.expect_download() as download_info:
                self.logger.debug(f"Clicking on {self.shapefile_text}")
                loc = (
                    page.locator("strong")
                    .filter(has_text=self.shapefile_text)
                    .get_by_role("link")
                )
                self.logger.debug(f"Clicking on {loc}")
                loc.click()
            download = download_info.value
            destination = self.save_source_path(download.suggested_filename)
            download.save_as(destination)
            self.logger.info(
                f"Downloaded {self.etl_name()} shape files to {destination}"
            )


if __name__ == "__main__":
    etl = CalEnviroScreenETL()
    etl.run(use_cache=True)
