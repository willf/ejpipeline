import pathlib
import data_pipeline.utils as utils


class BaseETL:
    def etl_name(self):
        return pathlib.Path(__file__).stem

    def extract(self):
        return (None, None)

    def transform(self, data):
        return data

    def save_source(self, data, source_filename):
        etl_name = self.etl_name()
        source_path = utils.get_source_sink_path(etl_name)
        # ensure the directory exists
        source_path.mkdir(parents=True, exist_ok=True)
        source_filename = source_path / source_filename
        with open(source_filename, "wb") as f:
            f.write

    def load(self, data, sink_filename):
        etl_name = self.etl_name()
        sink_path = utils.get_sink_path(etl_name)
        # ensure the directory exists

        sink_path.mkdir(parents=True, exist_ok=True)
        sink_filename = sink_path / sink_filename
        with open(sink_filename, "wb") as f:
            f.write(data)

    def run(self, use_cache=True):
        # if using cache, check if there are any files in the sink directory
        etl_name = self.etl_name()
        sink_path = utils.get_sink_path(etl_name)
        if use_cache and sink_path.exists() and len(list(sink_path.iterdir())) > 0:
            return False
        data, filename = self.extract()
        if data is None:
            return False
        self.save_source(data, filename)
        transformed_data = self.transform(data)
        self.load(transformed_data, filename)
        return True
