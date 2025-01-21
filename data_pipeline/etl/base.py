import pathlib
import data_pipeline.utils as utils

MY_DIRECTORY = pathlib.Path(__file__).parent


class BaseETL:
    def __init__(self):
        self.logger = utils.get_logger(__name__)

    @classmethod
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
        # Note this just checks the sink directory
        destination = utils.get_source_sink_path(self.etl_name())
        return destination.exists() and len(list(destination.iterdir())) > 0

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass

    def dry_run(self):
        cached = self.already_etled()
        if cached:
            self.logger.info(
                f"Dry run: {self.etl_name()}. Data already processed. Check {utils.get_etl_path(self.etl_name())}"
            )
        else:
            self.logger.info(f"Fake running {self.etl_name()}")

    def run(self, use_cache=True, dry_run=False):
        if dry_run:
            self.dry_run()
            return True
        if use_cache and self.already_etled():
            self.logger.info(
                f"Data already processed for {self.etl_name()}. Check {utils.get_etl_path(self.etl_name())}"
            )
            return
        self.extract()
        self.transform()
        self.load()
        return True

    def run_all(self, use_cache=True, dry_run=False, etl_names=None):
        message = "Running ETLs"
        classes = set(utils.etl_classes(MY_DIRECTORY, self.__class__))
        input_class_names = [cls.etl_name() for cls in classes]
        if etl_names:
            classes = [cls for cls in classes if cls.etl_name() in etl_names]
            not_found_names = [
                name for name in etl_names if name not in input_class_names
            ]
            if not_found_names:
                for name in not_found_names:
                    self.logger.error(f"ETL not found: {name}")
        else:
            message = "Running all ETLs"
        if dry_run:
            message = "Dry run: " + message
        self.logger.info(message)
        for cls in classes:
            etl = cls()
            etl.run(use_cache=use_cache, dry_run=dry_run)
