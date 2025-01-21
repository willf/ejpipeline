from data_pipeline.etl.base import BaseETL
from data_pipeline import utils
from playwright.sync_api import sync_playwright


class EpaAirToxScreenETL(BaseETL):
    def __init__(self):
        self.directory_links = [
            "https://gaftp.epa.gov/rtrmodeling_public/AirToxScreen/2020/Cancer/BySource/",
            # "https://gaftp.epa.gov/rtrmodeling_public/AirToxScreen/2020/Cancer/ByPollutant/",
            # "https://gaftp.epa.gov/rtrmodeling_public/AirToxScreen/2020/Ambient%20Concentrations/",
            # "https://gaftp.epa.gov/rtrmodeling_public/AirToxScreen/2020/Exposure%20Concentrations/",
            # "https://gaftp.epa.gov/rtrmodeling_public/AirToxScreen/2020/Pollutant%20Summaries/",
        ]
        self.direct_links = [
            "https://www.epa.gov/system/files/documents/2024-05/airtoxscreen_2020-tsd.pdf",
            "https://www.epa.gov/system/files/other-files/2024-06/2020-airtoxscreen-supplemental-data-files.zip",
            "https://www.epa.gov/system/files/other-files/2022-12/2019%20AirToxScreen%20Supplemental%20Data%20files.zip",
            "https://www.epa.gov/system/files/documents/2023-02/AirToxScreen_2018%20TSD.pdf",
            "https://www.epa.gov/system/files/other-files/2022-10/2018%20AirToxScreen%20Supplemental%20Data%20files.zip",
            "https://www.epa.gov/system/files/documents/2022-10/EtO%20sterilizer%202018%20emissions%20calcs.pdf",
            "https://www.epa.gov/system/files/documents/2024-05/emissions_updates_2020_airtoxscreen.pdf",
            "https://www.epa.gov/system/files/other-files/2024-05/point_fac_2020_mapapp.xlsx",
            "https://www.epa.gov/system/files/other-files/2024-05/railyards_2020_mapapp.xlsx",
            "https://www.epa.gov/system/files/other-files/2024-05/airport_runway_2020_mapapp.xlsx",
            "https://www.epa.gov/system/files/other-files/2024-05/airport_nonrunway_2020_mapapp.xlsx",
            "https://www.epa.gov/system/files/other-files/2024-05/airtoxscreen_2020_emissions_county_sourcegroup.zip",
            "https://www.epa.gov/system/files/documents/2025-01/national_cancerrisk_by_county_srcgrp.xlsx",
            "https://www.epa.gov/system/files/documents/2025-01/national_cancerrisk_by_county_poll.xlsx",
            "https://www.epa.gov/system/files/documents/2025-01/national_cancerrisk_by_state_srcgrp.xlsx",
            "https://www.epa.gov/system/files/documents/2025-01/national_cancerrisk_by_state_poll.xlsx",
        ]
        self.logger = utils.get_logger(__name__)

    @classmethod
    def etl_name(cls):
        return "epa_air_tox_screen"

    def extract(self):
        self.logger.info(f"Extracting data from {self.etl_name()}")
        amt_downloaded = 0
        files_downloaded = 0
        # for file in self.direct_links:
        #     destination = self.save_source_path(
        #         file.removeprefix("https://www.epa.gov/")
        #     )
        #     downloaded_amount = utils.download_url(file, destination)
        #     files_downloaded += 1
        #     amt_downloaded += downloaded_amount
        #     self.logger.info(
        #         f"Downloaded {self.etl_name()} data files to {destination}"
        #     )
        for directory in self.directory_links:
            self.logger.info(f"Extracting data from {directory} for {self.etl_name()}")
            urls = []
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                urls = list(utils.ftp_like_download_list(directory, browser))
            for url in urls:
                if url.endswith("/"):
                    print(f"Skipping directory {url}")
                    continue
                destination = self.save_source_path(
                    url.removeprefix("https://gaftp.epa.gov/")
                )
                print(f"Downloading {url} to {destination}")
                downloaded_amount = utils.download_url(url, destination, verify=False)
                downloaded_amount = 0
                files_downloaded += 1

                amt_downloaded += downloaded_amount
            self.logger.info(
                f"Downloaded {self.etl_name()} data files to {destination}"
            )
        self.logger.info(
            f"Downloaded {files_downloaded} {self.etl_name()} data files to {destination}"
        )
        return amt_downloaded

    def transform(self):
        # self.logger.info(f"Transforming data for {self.etl_name()}")
        # Add transformation logic here
        pass

    def load(self):
        # self.logger.info(f"Loading data for {self.etl_name()}")
        # Add loading logic here
        pass


if __name__ == "__main__":
    etl = EpaAirToxScreenETL()
    etl.run(use_cache=True)
