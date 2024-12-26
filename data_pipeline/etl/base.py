import pathlib
import data_pipeline.utils as utils
import click

MY_DIRECTORY = pathlib.Path(__file__).parent


class BaseETL:
    def __init__(self):
        self.logger = utils.get_logger(__name__)

    def etl_name(self):
        return pathlib.Path(__file__).stem

    def save_source_path(self, source_filename):
        etl_name = self.etl_name()
        source_path = utils.get_source_sink_path(etl_name)
        # ensure the directory exists
        source_path.mkdir(parents=True, exist_ok=True)
        return source_path / source_filename

    # The following are the methods that need to be implemented by the child classes

    def already_etled(self):
        return False

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass

    def run(self, use_cache=True):
        if use_cache and self.already_etled():
            self.logger.info(
                f"Data already processed for {self.etl_name()}. Check {utils.get_etl_path(self.etl_name())}"
            )
            return
        self.extract()
        self.transform()
        self.load()
        return True

    def run_all(self, use_cache=True):
        classes = set(utils.etl_classes(MY_DIRECTORY))
        self.logger.info(f"Running ETLs")
        for cls in classes:
            etl = cls()
            self.logger.info(f"Running {etl.etl_name()}")
            etl.run(use_cache=use_cache)


@click.command()
@click.option("--force", is_flag=True, default=False, help="Force the ETL to rerun")
def run_all(force):
    etl = BaseETL()
    use_cache = not force
    etl.logger.info(f"Calling run_all with force={force}")
    BaseETL().run_all(use_cache=use_cache)


if __name__ == "__main__":
    run_all()
