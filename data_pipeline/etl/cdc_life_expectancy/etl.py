from data_pipeline.etl.base import BaseETL

from data_pipeline import utils
import requests


class CDCLifeExpectancyETL(BaseETL):
    def __init__(self):
        self.usa_file_url: str = "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NVSS/USALEEP/CSV/US_A.CSV"
        self.wisconsin_file_url: str = "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NVSS/USALEEP/CSV/WI_A.CSV"
        self.maine_file_url: str = "https://ftp.cdc.gov/pub/Health_Statistics/NCHS/Datasets/NVSS/USALEEP/CSV/ME_A.CSV"

        self.logger = utils.get_logger(__name__)

    @classmethod
    def etl_name(self):
        return "cdc_life_expectancy"

    def extract(self):
        self.logger.info(f"Extracting data from {self.etl_name()}")
        self.logger.debug(f"Extracting data from {self.usa_file_url}")
        # Save USA file by getting the content of the file and saving it
        usa_response = requests.get(self.usa_file_url)
        usa_destination = self.save_source_path("USA_A.csv")
        with open(usa_destination, "wb") as f:
            f.write(usa_response.content)
        self.logger.info(
            f"Downloaded USA {self.etl_name()} data files to {usa_destination}"
        )
        # Save Wisconsin file by getting the content of the file and saving it
        wisconsin_response = requests.get(self.wisconsin_file_url)
        wisconsin_destination = self.save_source_path("WI_A.csv")
        with open(wisconsin_destination, "wb") as f:
            f.write(wisconsin_response.content)
        self.logger.info(
            f"Downloaded Wisconsin {self.etl_name()} data files to {wisconsin_destination}"
        )
        # Save Maine file by getting the content of the file and saving it
        maine_response = requests.get(self.maine_file_url)
        maine_destination = self.save_source_path("ME_A.csv")
        with open(maine_destination, "wb") as f:
            f.write(maine_response.content)
        self.logger.info(
            f"Downloaded Maine {self.etl_name()} data files to {maine_destination}"
        )


if __name__ == "__main__":
    etl = CDCLifeExpectancyETL()
    etl.run(use_cache=True)
