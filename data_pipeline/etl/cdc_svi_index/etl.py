from data_pipeline.etl.base import BaseETL
from data_pipeline import utils


class CdcSviIndexETL(BaseETL):
    def __init__(self):
        self.source_url = (
            "https://svi.cdc.gov/Documents/Data/2018_SVI_Data/CSV/SVI2018_US.csv"
        )
        self.file_name = "SVI2018_US.csv"
        self.logger = utils.get_logger(__name__)

    @classmethod
    def etl_name(cls):
        return "cdc_svi_index"

    def extract(self):
        self.logger.info(f"Extracting data from {self.etl_name()}")
        destination = self.save_source_path(self.file_name)
        downloaded_amount = utils.download_url(self.source_url, destination)
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
    etl = CdcSviIndexETL()
    etl.run(use_cache=True)
