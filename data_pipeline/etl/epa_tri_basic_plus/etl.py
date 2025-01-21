from data_pipeline.etl.base import BaseETL
from data_pipeline import utils
from datetime import datetime
from requests.exceptions import HTTPError


class EpaTriBasicPlusETL(BaseETL):
    def __init__(self):
        self.pdfs = [
            "https://www.epa.gov/system/files/documents/2024-09/file_type_1a_1.pdf",
            "https://www.epa.gov/system/files/documents/2024-09/file_type_1b_1.pdf",
            "https://www.epa.gov/system/files/documents/2024-09/file_type_2a_1.pdf",
            "https://www.epa.gov/system/files/documents/2024-09/file_type_2b_1.pdf",
            "https://www.epa.gov/system/files/documents/2024-09/file_type_3a_1.pdf",
            "https://www.epa.gov/system/files/documents/2024-09/file_type_3b_1.pdf",
            "https://www.epa.gov/system/files/documents/2024-09/file_type_3c_1.pdf",
            "https://www.epa.gov/system/files/documents/2024-09/file_type_4_1.pdf",
            "https://www.epa.gov/system/files/documents/2024-09/file_type_5_1.pdf",
            "https://www.epa.gov/system/files/documents/2024-09/file_type_6_1.pdf",
        ]
        self.destination_dir = utils.get_source_sink_path(self.etl_name())
        self.logger = utils.get_logger(__name__, level="DEBUG")

    @classmethod
    def etl_name(cls):
        return "epa_tri_basic_plus"

    def already_etled(self):
        destination = self.destination_dir
        return destination.exists() and len(list(destination.iterdir())) > 0

    def scrape_zips(self, year: int = 2020):
        url = f"https://www3.epa.gov/tri/current2/US_{year}.zip"
        destination = self.destination_dir / f"US_{year}.zip"
        self.logger.debug(f"Downloading {url} to {destination}")
        try:
            utils.download_url(url, destination, force=False)
        except HTTPError as http_err:
            self.logger.error(f"HTTP error occurred: {http_err}")
        except Exception as err:
            self.logger.error(f"Other error occurred: {err}")

    def extract(self):
        self.logger.info(f"Extracting data for {self.etl_name()}")
        for pdf in self.pdfs:
            destination = self.destination_dir / pdf.split("/")[-1]
            if destination.exists():
                self.logger.debug(f"{destination} already exists. Skipping.")
                continue
            self.logger.debug(f"Downloading {pdf} to {destination}")
            utils.download_url(pdf, destination, force=False)
        # get current year
        this_year = datetime.now().year
        years = range(1987, this_year + 1)
        for year in years:
            self.scrape_zips(year)

    def transform(self):
        # self.logger.info(f"Transforming data for {self.etl_name()}")
        # Add transformation logic here
        pass

    def load(self):
        # self.logger.info(f"Loading data for {self.etl_name()}")
        # Add loading logic here
        pass


if __name__ == "__main__":
    etl = EpaTriBasicPlusETL()
    etl.run(use_cache=True)
