from data_pipeline.etl.base import BaseETL
from data_pipeline import utils


class ChildOpportunityIndexETL(BaseETL):
    def __init__(self):
        self.source_url = "https://data.diversitydatakids.org/datastore/zip/f16fff12-b1e5-4f60-85d3-3a0ededa30a0?format=csv"
        self.file_name = "raw.csv"
        self.logger = utils.get_logger(__name__)

    @classmethod
    def etl_name(cls):
        return "child_opportunity_index"

    def extract(self):
        self.logger.info(f"Extracting data from {self.etl_name()}")
        destination = self.save_source_path(self.file_name)
        downloaded_amount = utils.download_via_download(self.source_url, destination)
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
    etl = ChildOpportunityIndexETL()
    etl.run(use_cache=True)
