from data_pipeline.etl.base import BaseETL
from data_pipeline import utils


class CDCPlacesETL(BaseETL):
    def __init__(self):
        self.source_url = "https://chronicdata.cdc.gov/api/views/cwsq-ngmh/rows.csv?accessType=DOWNLOAD"
        self.file_name = (
            "PLACES__Local_Data_for_Better_Health__Census_Tract_Data_2024_release.csv"
        )
        self.logger = utils.get_logger(__name__)

    @classmethod
    def etl_name(cls):
        return "cdc_places"

    def already_etled(self):
        destination = utils.get_source_sink_path(self.etl_name())
        return destination.exists() and len(list(destination.iterdir())) > 0

    def extract(self):
        self.logger.info(f"Extracting data from {self.etl_name()}")
        destination = self.save_source_path(self.file_name)
        downloaded_amount = utils.download_url(
            self.source_url, destination, estimated_size=(1024**2) * 700
        )
        self.logger.info(f"Downloaded {self.etl_name()} data files to {destination}")
        return downloaded_amount

    def transform(self):
        # self.logger.info(f"Transforming data for {self.etl_name()}")
        # Add transformation logic here
        pass

    def load(self):
        # self.logger.info(f"Loading data for {self.etl_name()}")
        # Add loading logic here
        pass


if __name__ == "__main__":
    etl = CDCPlacesETL()
    etl.run(use_cache=True)
