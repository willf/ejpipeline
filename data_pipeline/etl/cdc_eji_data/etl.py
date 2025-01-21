
from data_pipeline.etl.base import BaseETL
from data_pipeline import utils

class CdcEjiDataETL(BaseETL):
    def __init__(self):
        self.source_url = None # TODO: Add the source URL
        self.file_name = None # TODO: Add the file name
        self.logger = utils.get_logger(__name__)

    @classmethod
    def etl_name(cls):
        return "cdc_eji_data"

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
    etl = CdcEjiDataETL()
    etl.run(use_cache=True)
